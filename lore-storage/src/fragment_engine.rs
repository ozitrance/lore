// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::cmp::min;
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use lore_transport::StorageSession;
use tokio::task::JoinSet;

use crate::compress::FRAGMENT_SIZE_THRESHOLD;
use crate::concurrency::FRAGMENT_SIZE_EXPECTED;
use crate::concurrency::FRAGMENT_SIZE_MINIMUM;
use crate::error::StorageError;
use crate::fragment_flags::FragmentFlags;
use crate::hash;
use crate::immutable_store::ImmutableStore;
use crate::options::WriteOptions;
use crate::typed_bytes::TypedBytes;
use crate::types::Address;
use crate::types::Context;
use crate::types::Fragment;
use crate::types::FragmentReference;
use crate::types::Partition;
use crate::write::store_fragment;

/// Splits content into fragments using `FastCDC` and stores them via
/// [`store_fragment`].
///
/// For each chunk the function calls [`store_fragment`] directly with the
/// provided store, partition, and optional remote session. This replaces the
/// previous closure-based `store_fn` pattern.
#[allow(clippy::too_many_arguments)]
pub async fn write_fragmented(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    context: Context,
    buffer: Bytes,
    flags: WriteOptions,
    hash_only: bool,
    remote_session: Option<Arc<StorageSession>>,
    tracker: Option<Arc<crate::write_tracker::WriteTracker>>,
) -> Result<(Address, Fragment), StorageError> {
    // Raw data, use content defined chunking with FastCDC
    let size = buffer.len();
    let mut chunk_offset = 0usize;
    let mut tasks = JoinSet::<Result<(usize, usize, Address), StorageError>>::new();
    let mut chunk_index = 0usize;

    let chunker = Arc::new({
        // SAFETY: we await on all spawned tasks using this chunker before buffer
        // is dropped, so the lifetime will not escape the scope of the buffer.
        let buffer: &[u8] = unsafe { extend_lifetime(buffer.as_ref()) };
        fastcdc::v2020::FastCDC::with_level(
            buffer,
            FRAGMENT_SIZE_MINIMUM as u32,
            FRAGMENT_SIZE_EXPECTED as u32,
            FRAGMENT_SIZE_THRESHOLD as u32,
            fastcdc::v2020::Normalization::Level1,
        )
    });

    lore_base::lore_trace!(
        "Write and fragment buffer to immutable store: {size} bytes representing {size} bytes (flags {flags:?})",
    );
    while chunk_offset < size {
        let chunk_remain = size - chunk_offset;
        let chunk_end = if flags.fixed_size_chunk > 0 {
            chunk_offset + std::cmp::min(flags.fixed_size_chunk, chunk_remain)
        } else {
            // FastCDC chunking is CPU-bound (rolling hash over the buffer).
            // Run it on the shared compute pool alongside compression so it
            // doesn't contend with blocking IO on tokio's blocking pool.
            let chunker = chunker.clone();
            // The chunker borrows `buffer` via a forged `'static` slice (see
            // `extend_lifetime` above). The rayon task is detached and is not
            // cancelled if this future is dropped at the `rx.await` below, so
            // we must keep the buffer allocation alive for the whole task.
            // `Bytes::clone` bumps the refcount with a stable data pointer,
            // guaranteeing the slice stays valid until the task finishes.
            let buffer_guard = buffer.clone();
            let (tx, rx) = tokio::sync::oneshot::channel();
            lore_base::runtime::compute_pool().spawn(move || {
                let _ = tx.send(chunker.cut(chunk_offset, chunk_remain));
                drop(buffer_guard);
            });
            let (_, chunk_end) = rx
                .await
                .map_err(|e| StorageError::internal_with_context(e, "chunker task failed"))?;
            chunk_end
        };
        let chunk_size = chunk_end - chunk_offset;
        let chunk_buffer = buffer.slice(chunk_offset..(chunk_offset + chunk_size));

        let fragment = Fragment {
            flags: flags.into(),
            size_payload: chunk_size as u32,
            size_content: chunk_size as u64,
        };

        if chunk_offset == 0 && chunk_size == size {
            // Everything was put in a single fragment
            let chunk_buffer = if flags.clone_buffer {
                Bytes::copy_from_slice(chunk_buffer.as_ref())
            } else {
                chunk_buffer
            };
            let hash = hash::hash_slice(chunk_buffer.as_ref());
            let permit =
                crate::concurrency::acquire_fragment_memory_permit(chunk_buffer.len()).await;
            let result = store_fragment(
                store,
                partition,
                Address { context, hash },
                fragment,
                chunk_buffer,
                flags.local_cache_priority,
                remote_session,
                tracker,
                permit,
            )
            .await?;
            return Ok((result.address, result.fragment));
        }

        let store = store.clone();
        let session = remote_session.clone();
        let task_tracker = tracker.clone();
        lore_base::lore_spawn!(tasks, async move {
            let chunk_buffer = if flags.clone_buffer {
                Bytes::copy_from_slice(chunk_buffer.as_ref())
            } else {
                chunk_buffer
            };
            let hash = hash::hash_slice(chunk_buffer.as_ref());
            let chunk_address = if hash_only {
                Address { context, hash }
            } else {
                let permit =
                    crate::concurrency::acquire_fragment_memory_permit(chunk_buffer.len()).await;
                let result = store_fragment(
                    store,
                    partition,
                    Address { context, hash },
                    fragment,
                    chunk_buffer,
                    flags.local_cache_priority,
                    session,
                    task_tracker,
                    permit,
                )
                .await?;
                result.address
            };
            Ok((chunk_index, chunk_offset, chunk_address))
        });

        chunk_offset += chunk_size;
        chunk_index += 1;
    }
    drop(chunker);
    drop(buffer);

    let mut list_buffer =
        BytesMut::with_capacity(tasks.len() * std::mem::size_of::<FragmentReference>());
    let list = unsafe {
        std::slice::from_raw_parts_mut(
            list_buffer.as_mut_ptr().cast::<FragmentReference>(),
            tasks.len(),
        )
    };

    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        match result
            .map_err(|e| StorageError::internal_with_context(e, "task failure"))
            .and_then(|r| r)
        {
            Ok((chunk_index, chunk_content_offset, chunk_address)) => {
                list[chunk_index].hash = chunk_address.hash;
                list[chunk_index].offset_content = chunk_content_offset as u64;
            }
            Err(err) => {
                failure = failure.or(Some(err));
            }
        }
    }

    if let Some(err) = failure {
        return Err(err);
    }

    unsafe {
        list_buffer.set_len(list_buffer.capacity());
    }

    // This never needs to be cloned, it's a unique immutable buffer
    let mut flags = flags;
    flags.clone_buffer = false;

    write_fragmentlist(
        store,
        partition,
        context,
        list_buffer.freeze(),
        size,
        flags,
        hash_only,
        remote_session,
        tracker,
    )
    .await
}

/// Helper function to write a list of fragment references
#[allow(clippy::too_many_arguments)]
async fn write_fragmentlist_impl(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    context: Context,
    buffer: Bytes,
    content_size: usize,
    flags: WriteOptions,
    hash_only: bool,
    remote_session: Option<Arc<StorageSession>>,
    tracker: Option<Arc<crate::write_tracker::WriteTracker>>,
) -> Result<(Address, Fragment), StorageError> {
    let size = buffer.len();

    if size <= FRAGMENT_SIZE_THRESHOLD {
        let hash = hash::hash_slice(buffer.as_ref());
        let fragment = Fragment {
            flags: flags.as_u32() | FragmentFlags::PayloadFragmented,
            size_payload: size as u32,
            size_content: content_size as u64,
        };
        if hash_only {
            Ok((Address { context, hash }, fragment))
        } else {
            let permit = crate::concurrency::acquire_fragment_memory_permit(buffer.len()).await;
            let result = store_fragment(
                store,
                partition,
                Address { context, hash },
                fragment,
                buffer,
                true, /* Fragment lists have local priority */
                remote_session,
                tracker,
                permit,
            )
            .await?;
            Ok((result.address, result.fragment))
        }
    } else {
        // Fixed size chunking for fragment list
        let mut tasks = JoinSet::<Result<(usize, usize, Address), StorageError>>::new();
        let mut chunk_index = 0usize;
        let mut chunk_offset = 0;
        let mut chunk_content_offset = 0;

        let max_fragment_ref_count =
            FRAGMENT_SIZE_THRESHOLD / std::mem::size_of::<FragmentReference>();
        let max_chunk_size = std::mem::size_of::<FragmentReference>() * max_fragment_ref_count;

        let buffer = buffer.to_aligned::<FragmentReference>();
        let fragment_references = buffer.as_type_slice::<FragmentReference>();

        while chunk_offset < size {
            let chunk_size = min(size - chunk_offset, max_chunk_size);
            let chunk_buffer = buffer.slice(chunk_offset..(chunk_offset + chunk_size));

            let chunk_fragment_ref_index = chunk_offset / std::mem::size_of::<FragmentReference>();
            let next_fragment_ref_index =
                chunk_fragment_ref_index + (chunk_size / std::mem::size_of::<FragmentReference>());

            let chunk_content_size = if chunk_offset + chunk_size < size {
                fragment_references[next_fragment_ref_index].offset_content as usize
            } else {
                content_size
            } - fragment_references[chunk_fragment_ref_index]
                .offset_content as usize;

            let fragment = Fragment {
                flags: flags.as_u32() | FragmentFlags::PayloadFragmented,
                size_payload: chunk_size as u32,
                size_content: chunk_content_size as u64,
            };

            let store = store.clone();
            let session = remote_session.clone();
            let task_tracker = tracker.clone();
            lore_base::lore_spawn!(tasks, async move {
                let hash = hash::hash_slice(chunk_buffer.as_ref());
                let chunk_address = if hash_only {
                    Address { context, hash }
                } else {
                    let permit =
                        crate::concurrency::acquire_fragment_memory_permit(chunk_buffer.len())
                            .await;
                    let result = store_fragment(
                        store,
                        partition,
                        Address { context, hash },
                        fragment,
                        chunk_buffer,
                        flags.local_cache_priority,
                        session,
                        task_tracker,
                        permit,
                    )
                    .await?;
                    result.address
                };
                Ok((chunk_index, chunk_content_offset, chunk_address))
            });

            chunk_content_offset += chunk_content_size;
            chunk_offset += chunk_size;
            chunk_index += 1;
        }
        drop(buffer);

        let mut list_buffer =
            BytesMut::with_capacity(tasks.len() * std::mem::size_of::<FragmentReference>());
        let list = unsafe {
            std::slice::from_raw_parts_mut(
                list_buffer.as_mut_ptr().cast::<FragmentReference>(),
                tasks.len(),
            )
        };

        let mut failure = None;
        while let Some(result) = tasks.join_next().await {
            match result
                .map_err(|e| StorageError::internal_with_context(e, "task failure"))
                .and_then(|r| r)
            {
                Ok((chunk_index, chunk_content_offset, chunk_address)) => {
                    list[chunk_index].hash = chunk_address.hash;
                    list[chunk_index].offset_content = chunk_content_offset as u64;
                }
                Err(err) => {
                    failure = failure.or(Some(err));
                }
            }
        }

        if let Some(err) = failure {
            return Err(err);
        }

        unsafe {
            list_buffer.set_len(list_buffer.capacity());
        }
        let buffer = list_buffer.freeze();

        write_fragmentlist(
            store,
            partition,
            context,
            buffer,
            content_size,
            flags,
            hash_only,
            remote_session,
            tracker,
        )
        .await
    }
}

/// Helper function to enforce compiler breaking the async recursion chain
/// and pinning the future on heap allocation.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn write_fragmentlist(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    context: Context,
    buffer: Bytes,
    content_size: usize,
    flags: WriteOptions,
    hash_only: bool,
    remote_session: Option<Arc<StorageSession>>,
    tracker: Option<Arc<crate::write_tracker::WriteTracker>>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(Address, Fragment), StorageError>> + Send>,
> {
    Box::pin(write_fragmentlist_impl(
        store,
        partition,
        context,
        buffer,
        content_size,
        flags,
        hash_only,
        remote_session,
        tracker,
    ))
}

/// Unsafe extension of lifetime. Caller must guarantee the reference outlives
/// all uses. Used for `FastCDC`'s borrow of the buffer slice.
pub(crate) unsafe fn extend_lifetime<T>(data: &T) -> &'static T
where
    T: ?Sized,
{
    unsafe { &*(data as *const T) }
}
