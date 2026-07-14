// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `lore_revision_tree_add` — add a leaf or empty directory child under a
//! parent node. `kind` is a `u32` matching the `LoreNodeType` encoding the
//! read verbs emit (DIRECTORY=0, FILE=1, LINK=2); the verb rejects any
//! other value with `LORE_ERROR_CODE_INVALID_ARGUMENTS`.

use lore_base::error::InvalidArguments;
use lore_base::types::Address;
use lore_error_set::prelude::*;
use lore_macro::LoreArgs;
use lore_revision::event::EventError;
use lore_revision::event::LoreErrorCode;
use lore_revision::event::LoreEvent;
use lore_revision::event::revision_tree::LoreRevisionTreeAddCompleteEventData;
use lore_revision::interface::LoreError;
use lore_revision::interface::LoreNodeType;
use lore_revision::interface::LoreString;
use lore_revision::node::INVALID_NODE;
use lore_revision::node::Node;
use lore_revision::node::NodeFlags;
use lore_revision::node::NodeID;
use lore_revision::node::NodeIDExt;
use lore_revision::node::ROOT_NODE;
use lore_storage::hash::hash_string;
use serde::Deserialize;
use serde::Serialize;

use crate::call_delegation::dispatch_call;
use crate::interface::LoreEventCallback;
use crate::interface::LoreGlobalArgs;
use crate::revision_tree::call::revision_tree_call;
use crate::revision_tree::handle::LoreRevisionTree;

/// Arguments for `lore_revision_tree_add`.
#[repr(C)]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, LoreArgs)]
#[handler(add_impl)]
pub struct LoreRevisionTreeAddArgs {
    /// Per-call correlation id echoed back in events
    pub id: u64,
    /// Loaded revision-tree handle to mutate
    pub handle: LoreRevisionTree,
    /// Parent node the new child is added under
    pub parent_node_id: NodeID,
    /// UTF-8 name of the new child within its parent
    pub name: LoreString,
    /// `LoreNodeType` encoding: DIRECTORY=0, FILE=1, LINK=2
    pub kind: u32,
    /// POSIX permission bits for the new node
    pub mode: u16,
    /// Content size in bytes (leaf nodes)
    pub size: u64,
    /// Content address `(hash, file_id context)` of the new node
    pub address: Address,
}

#[error_set]
enum AddError {
    InvalidArguments,
}

impl EventError for AddError {
    fn translated(&self) -> LoreError {
        match self {
            AddError::InvalidArguments(_) => LoreError::InvalidArguments,
            AddError::Internal(_) => LoreError::Internal,
        }
    }

    fn inner(&self) -> String {
        self.to_string()
    }
}

fn invalid(reason: &str) -> AddError {
    AddError::from(InvalidArguments {
        reason: reason.into(),
    })
}

fn emit_add_complete(id: u64, node_id: NodeID, error_code: LoreErrorCode) {
    LoreEvent::RevisionTreeAddComplete(LoreRevisionTreeAddCompleteEventData {
        id,
        node_id,
        error_code,
    })
    .send();
}

/// Add a leaf or empty directory child under a parent node.
///
/// On success the caller receives `LORE_EVENT_REVISION_TREE_ADD_COMPLETE`
/// carrying the newly-assigned node id and `error_code = NONE`, before
/// `Complete {status: 0}`. The address is a value returned by
/// `lore_storage_put`; this verb does not move bytes. The parent must be a
/// directory in the handle's own tree — a link parent is not followed, since a
/// link target is a frozen revision this handle cannot mutate. A name that
/// already exists under the parent is rejected at call time with
/// `INVALID_ARGUMENTS` (delete or modify the existing node instead).
///
/// Per-kind argument meaning: FILE honors `mode`, `size`, and `address`
/// (`address.context` is the file id assigned by the caller). DIRECTORY
/// honors `mode` and requires a zero `size` and `address` — a directory's
/// hash and size are computed at commit. LINK stores the target
/// `(revision, repository)` in `address` (`hash` = target revision,
/// `context` = target repository), targets the linked tree's root, requires
/// a zero `size`, and rejects a zero target revision.
pub async fn add(
    globals: LoreGlobalArgs,
    args: LoreRevisionTreeAddArgs,
    callback: LoreEventCallback,
) -> i32 {
    dispatch_call(globals, args, callback, add_impl).await
}

async fn add_impl(
    globals: LoreGlobalArgs,
    args: LoreRevisionTreeAddArgs,
    callback: LoreEventCallback,
) -> i32 {
    let handle = args.handle;
    let miss_id = args.id;
    revision_tree_call(
        globals,
        callback,
        handle,
        args,
        add,
        move || {
            emit_add_complete(miss_id, INVALID_NODE, LoreErrorCode::InvalidArguments);
        },
        async move |internal, args: LoreRevisionTreeAddArgs| {
            let id = args.id;
            let state = internal.state();
            let fail = |reason: &str| {
                emit_add_complete(id, INVALID_NODE, LoreErrorCode::InvalidArguments);
                Err(invalid(reason))
            };

            if !args.parent_node_id.is_valid_or_root_node_id() {
                return fail("parent node id is invalid");
            }

            let Ok(name) = std::str::from_utf8(args.name.as_bytes()) else {
                return fail("name is not valid UTF-8");
            };
            if name.is_empty() {
                return fail("name is empty");
            }
            if name.contains('/') || name.contains('\\') {
                return fail("name must not contain path separators");
            }

            let flags = if args.kind == LoreNodeType::File as u32 {
                NodeFlags::File
            } else if args.kind == LoreNodeType::Directory as u32 {
                NodeFlags::NoFlags
            } else if args.kind == LoreNodeType::Link as u32 {
                NodeFlags::Link
            } else {
                return fail("kind is not a valid node type");
            };

            // Reject at call time what commit would silently overwrite or
            // could never resolve: a directory's hash and size are computed
            // by the commit rehash, and a link with a zero target revision
            // is dangling from birth.
            if args.kind == LoreNodeType::Directory as u32
                && (args.size != 0 || !args.address.hash.is_zero() || !args.address.context.is_zero())
            {
                return fail("a directory takes no size or address; both are computed at commit");
            }
            if args.kind == LoreNodeType::Link as u32 {
                if args.size != 0 {
                    return fail("a link takes no size");
                }
                if args.address.hash.is_zero() {
                    return fail("a link requires a target revision in address.hash");
                }
            }

            let Ok(parent) = state
                .node(internal.repository_context.clone(), args.parent_node_id)
                .await
            else {
                return fail("parent node id is unknown");
            };
            // A discarded slot keeps its name for history weaving and reads
            // back as a directory shape; the node itself is gone.
            if parent.is_discarded() {
                return fail("parent node id resolves to a deleted node");
            }
            if !parent.is_directory() {
                return fail("parent node is not a directory in the handle's own tree");
            }
            // Every non-root node has a non-empty name, so an empty name means
            // the id landed on an unallocated slot rather than a real node
            // (consistent with `node_info`).
            if args.parent_node_id != ROOT_NODE {
                match state
                    .node_name_clone(internal.repository_context.clone(), args.parent_node_id)
                    .await
                {
                    Ok(parent_name) if parent_name.is_empty() => {
                        return fail("parent node id does not resolve to a named node");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        emit_add_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                        return Err(AddError::internal_with_context(
                            error,
                            "State::node_name_clone",
                        ));
                    }
                }
            }

            let name_hash = hash_string(name);
            match state
                .find_subnode(
                    internal.repository_context.clone(),
                    args.parent_node_id,
                    name_hash,
                )
                .await
            {
                Ok(_existing) => {
                    return fail("name already exists under the parent");
                }
                Err(error) if error.is_node_not_found() => {}
                Err(error) => {
                    emit_add_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                    return Err(AddError::internal_with_context(error, "State::find_subnode"));
                }
            }

            let node = Node {
                flags: flags.bits(),
                name_hash,
                mode: args.mode,
                size: args.size,
                address: args.address,
                // A link targets the linked tree's root; `child` doubles as
                // the target node id on link nodes and ROOT_NODE is 0, so the
                // default is correct for every kind.
                ..Default::default()
            };

            let node_id = match state
                .node_add(
                    internal.repository_context.clone(),
                    args.parent_node_id,
                    node,
                    name,
                )
                .await
            {
                Ok(node_id) => node_id,
                Err(error) => {
                    emit_add_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                    return Err(AddError::internal_with_context(error, "State::node_add"));
                }
            };

            if let Err(error) = state
                .node_mark(
                    internal.repository_context.clone(),
                    node_id,
                    NodeFlags::StagedAdd,
                    true,
                )
                .await
            {
                emit_add_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                return Err(AddError::internal_with_context(error, "State::node_mark"));
            }

            emit_add_complete(id, node_id, LoreErrorCode::None);
            Ok(())
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use lore_base::types::Context;
    use lore_base::types::Hash;
    use lore_base::types::Partition;

    use super::*;
    use crate::revision_tree::handle as rt_handle;
    use crate::revision_tree::load::LoreRevisionTreeLoadArgs;
    use crate::revision_tree::load::load;
    use crate::storage::handle as storage_handle;
    use crate::storage::store::in_memory_for_tests;

    #[derive(Debug, Clone, PartialEq)]
    enum CapturedEvent {
        Error(u32),
        Complete(i32),
        RevisionTreeLoaded(u64),
        AddComplete(u64, NodeID, LoreErrorCode),
        Other(u32),
    }

    impl CapturedEvent {
        fn from_event(event: &LoreEvent) -> Self {
            match event {
                LoreEvent::Error(data) => Self::Error(data.error_type),
                LoreEvent::Complete(data) => Self::Complete(data.status),
                LoreEvent::RevisionTreeLoaded(data) => Self::RevisionTreeLoaded(data.handle_id),
                LoreEvent::RevisionTreeAddComplete(data) => {
                    Self::AddComplete(data.id, data.node_id, data.error_code)
                }
                other => Self::Other(other.discriminant()),
            }
        }
    }

    fn make_callback(sink: Arc<Mutex<Vec<CapturedEvent>>>) -> LoreEventCallback {
        Some(Box::new(move |event: &LoreEvent| {
            sink.lock().unwrap().push(CapturedEvent::from_event(event));
        }))
    }

    fn add_outcome(events: &[CapturedEvent], id: u64) -> Option<(NodeID, LoreErrorCode)> {
        events.iter().find_map(|event| match event {
            CapturedEvent::AddComplete(event_id, node_id, error_code) if *event_id == id => {
                Some((*node_id, *error_code))
            }
            _ => None,
        })
    }

    async fn load_handle(label: &str, repository: Partition) -> (LoreRevisionTree, u64) {
        let store = in_memory_for_tests(label).await;
        let store_handle = storage_handle::register(store);
        let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let status = load(
            LoreGlobalArgs::default(),
            LoreRevisionTreeLoadArgs {
                store: store_handle,
                repository,
                revision_hash: Hash::default(),
            },
            make_callback(sink.clone()),
        )
        .await;
        assert_eq!(status, 0, "load fixture must succeed");
        let id = sink
            .lock()
            .unwrap()
            .iter()
            .find_map(|event| match event {
                CapturedEvent::RevisionTreeLoaded(id) => Some(*id),
                _ => None,
            })
            .expect("load fixture must emit RevisionTreeLoaded");
        (LoreRevisionTree { handle_id: id }, store_handle.handle_id)
    }

    fn release(handle: LoreRevisionTree, store_handle_id: u64) {
        rt_handle::unregister(handle);
        storage_handle::unregister(crate::storage::handle::LoreStore {
            handle_id: store_handle_id,
        });
    }

    async fn run_add(
        handle: LoreRevisionTree,
        id: u64,
        args: LoreRevisionTreeAddArgs,
    ) -> (i32, Vec<CapturedEvent>) {
        let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let status = add(
            LoreGlobalArgs::default(),
            LoreRevisionTreeAddArgs {
                id,
                handle,
                ..args
            },
            make_callback(sink.clone()),
        )
        .await;
        let events = sink.lock().unwrap().clone();
        (status, events)
    }

    fn file_args(name: &str, size: u64, address: Address) -> LoreRevisionTreeAddArgs {
        LoreRevisionTreeAddArgs {
            parent_node_id: ROOT_NODE,
            name: LoreString::from_str(name),
            kind: LoreNodeType::File as u32,
            mode: 0o644,
            size,
            address,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn add_file_under_root_returns_node_id_and_marks_staged_add() {
        let (handle, store_handle_id) = load_handle("add-file", Partition::from([0x11u8; 16])).await;
        let address = Address {
            hash: Hash::from([0x42u8; 32]),
            context: Context::from([0x99u8; 16]),
        };

        let (status, events) = run_add(handle, 1, file_args("doc.md", 1234, address)).await;

        assert_eq!(status, 0, "adding a file must succeed, got {events:?}");
        let (node_id, error_code) = add_outcome(&events, 1).expect("AddComplete must fire");
        assert_eq!(error_code, LoreErrorCode::None);
        assert!(node_id.is_valid_node_id(), "must assign a real node id");
        assert!(events.contains(&CapturedEvent::Complete(0)));
        let add_pos = events
            .iter()
            .position(|event| matches!(event, CapturedEvent::AddComplete(..)))
            .expect("AddComplete must fire");
        let complete_pos = events
            .iter()
            .position(|event| matches!(event, CapturedEvent::Complete(_)))
            .expect("Complete must fire");
        assert!(
            add_pos < complete_pos,
            "AddComplete must fire before Complete, got {events:?}"
        );

        let (state, repository_context) = {
            let entry = rt_handle::REGISTRY
                .get(&handle.handle_id)
                .expect("handle registered");
            (entry.state(), entry.repository_context.clone())
        };
        let node = state
            .node(repository_context.clone(), node_id)
            .await
            .expect("added node must be readable");
        assert!(node.is_file());
        assert!(node.is_staged_add(), "added node must be marked StagedAdd");
        assert_eq!(node.mode, 0o644);
        assert_eq!(node.size, 1234);
        assert_eq!(node.address, address);
        assert_eq!(node.parent, ROOT_NODE);
        let name = state
            .node_name_clone(repository_context, node_id)
            .await
            .expect("added node must be named");
        assert_eq!(name, "doc.md");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_directory_then_child_file_nests_under_it() {
        let (handle, store_handle_id) = load_handle("add-dir", Partition::from([0x22u8; 16])).await;

        let (status, events) = run_add(
            handle,
            2,
            LoreRevisionTreeAddArgs {
                parent_node_id: ROOT_NODE,
                name: LoreString::from_str("docs"),
                kind: LoreNodeType::Directory as u32,
                mode: 0o755,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(status, 0, "adding a directory must succeed, got {events:?}");
        let (dir_id, error_code) = add_outcome(&events, 2).expect("AddComplete must fire");
        assert_eq!(error_code, LoreErrorCode::None);

        let (status, events) = run_add(
            handle,
            3,
            LoreRevisionTreeAddArgs {
                parent_node_id: dir_id,
                ..file_args("hello.md", 12, Address {
                    hash: Hash::from([0x24u8; 32]),
                    context: Context::from([0x77u8; 16]),
                })
            },
        )
        .await;
        assert_eq!(status, 0, "adding a nested file must succeed, got {events:?}");
        let (file_id, error_code) = add_outcome(&events, 3).expect("AddComplete must fire");
        assert_eq!(error_code, LoreErrorCode::None);

        let (state, repository_context) = {
            let entry = rt_handle::REGISTRY
                .get(&handle.handle_id)
                .expect("handle registered");
            (entry.state(), entry.repository_context.clone())
        };
        let file_node = state
            .node(repository_context.clone(), file_id)
            .await
            .expect("nested file must be readable");
        assert_eq!(file_node.parent, dir_id, "file must nest under the directory");
        let dir_node = state
            .node(repository_context, dir_id)
            .await
            .expect("directory must be readable");
        assert!(dir_node.is_directory());
        assert!(
            dir_node.is_staged(),
            "directory must carry the staged bit after a child add"
        );

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_duplicate_name_returns_invalid_arguments() {
        let (handle, store_handle_id) = load_handle("add-dup", Partition::from([0x33u8; 16])).await;
        let address = Address {
            hash: Hash::from([0x42u8; 32]),
            context: Context::from([0x88u8; 16]),
        };
        let (status, _events) = run_add(handle, 4, file_args("doc.md", 1, address)).await;
        assert_eq!(status, 0);

        let (status, events) = run_add(handle, 5, file_args("doc.md", 1, address)).await;
        assert_eq!(status, 1, "a duplicate name must fail");
        let (node_id, error_code) = add_outcome(&events, 5).expect("AddComplete must fire");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");
        assert_eq!(node_id, INVALID_NODE);
        assert!(events.contains(&CapturedEvent::Complete(1)));

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_rejects_invalid_kind_name_and_parent() {
        let (handle, store_handle_id) =
            load_handle("add-invalid", Partition::from([0x44u8; 16])).await;
        let address = Address {
            hash: Hash::from([0x42u8; 32]),
            context: Context::from([0x66u8; 16]),
        };

        let (status, events) = run_add(
            handle,
            6,
            LoreRevisionTreeAddArgs {
                kind: 7,
                ..file_args("doc.md", 1, address)
            },
        )
        .await;
        assert_eq!(status, 1, "an unknown kind must fail, got {events:?}");

        let (status, events) = run_add(handle, 7, file_args("", 1, address)).await;
        assert_eq!(status, 1, "an empty name must fail, got {events:?}");

        let (status, events) = run_add(handle, 8, file_args("docs/doc.md", 1, address)).await;
        assert_eq!(status, 1, "a separator in the name must fail, got {events:?}");

        let (status, events) = run_add(
            handle,
            9,
            LoreRevisionTreeAddArgs {
                parent_node_id: INVALID_NODE,
                ..file_args("doc.md", 1, address)
            },
        )
        .await;
        assert_eq!(status, 1, "an invalid parent must fail, got {events:?}");

        let (status, events) = run_add(
            handle,
            10,
            LoreRevisionTreeAddArgs {
                parent_node_id: 1_000_000,
                ..file_args("doc.md", 1, address)
            },
        )
        .await;
        assert_eq!(status, 1, "an unknown parent must fail, got {events:?}");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_under_a_file_parent_returns_invalid_arguments() {
        let (handle, store_handle_id) =
            load_handle("add-file-parent", Partition::from([0x55u8; 16])).await;
        let address = Address {
            hash: Hash::from([0x42u8; 32]),
            context: Context::from([0x55u8; 16]),
        };
        let (status, events) = run_add(handle, 11, file_args("doc.md", 1, address)).await;
        assert_eq!(status, 0);
        let (file_id, _) = add_outcome(&events, 11).expect("AddComplete must fire");

        let (status, events) = run_add(
            handle,
            12,
            LoreRevisionTreeAddArgs {
                parent_node_id: file_id,
                ..file_args("nested.md", 1, address)
            },
        )
        .await;
        assert_eq!(status, 1, "a file parent must fail");
        let (_, error_code) = add_outcome(&events, 12).expect("AddComplete must fire");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_directory_with_address_or_size_returns_invalid_arguments() {
        let (handle, store_handle_id) =
            load_handle("add-dir-strict", Partition::from([0x66u8; 16])).await;

        let (status, events) = run_add(
            handle,
            13,
            LoreRevisionTreeAddArgs {
                parent_node_id: ROOT_NODE,
                name: LoreString::from_str("docs"),
                kind: LoreNodeType::Directory as u32,
                mode: 0o755,
                size: 7,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(status, 1, "a directory with a size must fail, got {events:?}");

        let (status, events) = run_add(
            handle,
            14,
            LoreRevisionTreeAddArgs {
                parent_node_id: ROOT_NODE,
                name: LoreString::from_str("docs"),
                kind: LoreNodeType::Directory as u32,
                mode: 0o755,
                address: Address {
                    hash: Hash::from([0x42u8; 32]),
                    context: Context::default(),
                },
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            status, 1,
            "a directory with an address must fail, got {events:?}"
        );

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn add_on_unknown_handle_emits_add_complete_with_invalid_arguments() {
        let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let status = add(
            LoreGlobalArgs::default(),
            LoreRevisionTreeAddArgs {
                id: 15,
                handle: LoreRevisionTree::INVALID,
                parent_node_id: ROOT_NODE,
                name: LoreString::from_str("doc.md"),
                kind: LoreNodeType::File as u32,
                ..Default::default()
            },
            make_callback(sink.clone()),
        )
        .await;

        assert_eq!(status, 1, "adding against an unknown handle must fail");
        let events = sink.lock().unwrap().clone();
        let (node_id, error_code) = add_outcome(&events, 15)
            .expect("a handle miss must still emit AddComplete carrying the caller id");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");
        assert_eq!(node_id, INVALID_NODE);
        assert!(events.contains(&CapturedEvent::Complete(1)));
    }
}
