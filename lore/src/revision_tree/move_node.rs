// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `lore_revision_tree_move` — reparent and/or rename a node while
//! preserving its `file_id`, so the resulting revision graph records a
//! true move instead of a delete-plus-add pair. The Rust module is named
//! `move_node` because `move` is a reserved keyword; the C symbol stays
//! `lore_revision_tree_move`.

use std::pin::Pin;
use std::sync::Arc;

use lore_base::error::InvalidArguments;
use lore_error_set::prelude::*;
use lore_macro::LoreArgs;
use lore_revision::errors::StateErrors;
use lore_revision::event::EventError;
use lore_revision::event::LoreErrorCode;
use lore_revision::event::LoreEvent;
use lore_revision::event::revision_tree::LoreRevisionTreeMoveCompleteEventData;
use lore_revision::interface::LoreError;
use lore_revision::interface::LoreString;
use lore_revision::node::INVALID_NODE;
use lore_revision::node::Node;
use lore_revision::node::NodeBlock;
use lore_revision::node::NodeFlags;
use lore_revision::node::NodeID;
use lore_revision::node::NodeIDExt;
use lore_revision::node::ROOT_NODE;
use lore_revision::node::SiblingCycleGuard;
use lore_revision::repository::RepositoryContext;
use lore_revision::state::State;
use lore_storage::hash::hash_string;
use serde::Deserialize;
use serde::Serialize;

use crate::call_delegation::dispatch_call;
use crate::interface::LoreEventCallback;
use crate::interface::LoreGlobalArgs;
use crate::revision_tree::call::revision_tree_call;
use crate::revision_tree::handle::LoreRevisionTree;

/// Arguments for `lore_revision_tree_move`.
#[repr(C)]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, LoreArgs)]
#[handler(move_impl)]
pub struct LoreRevisionTreeMoveArgs {
    /// Per-call correlation id echoed back in events
    pub id: u64,
    /// Loaded revision-tree handle to mutate
    pub handle: LoreRevisionTree,
    /// Node to move; its `file_id` is preserved across the move
    pub node_id: NodeID,
    /// Parent node the moved node is reparented under
    pub destination_parent_id: NodeID,
    /// UTF-8 name the moved node takes at the destination
    pub dst_name: LoreString,
}

#[error_set]
enum MoveError {
    InvalidArguments,
}

impl EventError for MoveError {
    fn translated(&self) -> LoreError {
        match self {
            MoveError::InvalidArguments(_) => LoreError::InvalidArguments,
            MoveError::Internal(_) => LoreError::Internal,
        }
    }

    fn inner(&self) -> String {
        self.to_string()
    }
}

fn invalid(reason: &str) -> MoveError {
    MoveError::from(InvalidArguments {
        reason: reason.into(),
    })
}

fn emit_move_complete(id: u64, node_id: NodeID, error_code: LoreErrorCode) {
    LoreEvent::RevisionTreeMoveComplete(LoreRevisionTreeMoveCompleteEventData {
        id,
        node_id,
        error_code,
    })
    .send();
}

/// Recursively mark a moved directory's children with the move flag so the
/// commit delta records their changed paths (mirrors the stage path's
/// `mark_children_moved`). A child added in this handle stays an add.
fn mark_children_moved(
    state: Arc<State>,
    repository: Arc<RepositoryContext>,
    parent_node: NodeID,
    move_flag: NodeFlags,
) -> Pin<Box<dyn Future<Output = Result<(), StateErrors>> + Send>> {
    Box::pin(async move {
        let children = state.node_children(repository.clone(), parent_node).await?;
        for child_id in children {
            let child_node = state.node(repository.clone(), child_id).await?;
            let child_flag = if child_node.is_staged_add() {
                NodeFlags::StagedAdd
            } else {
                move_flag
            };
            state
                .node_mark(repository.clone(), child_id, child_flag, false)
                .await?;
            if child_node.is_directory() {
                mark_children_moved(state.clone(), repository.clone(), child_id, move_flag)
                    .await?;
            }
        }
        Ok(())
    })
}

/// Move a node between parents with optional rename (or rename within one).
///
/// On success the caller receives `LORE_EVENT_REVISION_TREE_MOVE_COMPLETE`
/// echoing the moved node id with `error_code = NONE`, before
/// `Complete {status: 0}`. The node's `file_id` (the `address.context` slot)
/// travels with it, so the revision graph records a true move instead of a
/// delete-plus-add pair. The root cannot be moved, the destination must be a
/// directory in the handle's own tree, a name that already exists at the
/// destination is rejected at call time, and a directory cannot be moved into
/// its own subtree — programmatic edits could otherwise construct a cycle no
/// filesystem would permit.
pub async fn move_node(
    globals: LoreGlobalArgs,
    args: LoreRevisionTreeMoveArgs,
    callback: LoreEventCallback,
) -> i32 {
    dispatch_call(globals, args, callback, move_impl).await
}

async fn move_impl(
    globals: LoreGlobalArgs,
    args: LoreRevisionTreeMoveArgs,
    callback: LoreEventCallback,
) -> i32 {
    let handle = args.handle;
    let miss_id = args.id;
    revision_tree_call(
        globals,
        callback,
        handle,
        args,
        move_node,
        move || {
            emit_move_complete(miss_id, INVALID_NODE, LoreErrorCode::InvalidArguments);
        },
        async move |internal, args: LoreRevisionTreeMoveArgs| {
            let id = args.id;
            let fail = |reason: &str| {
                emit_move_complete(id, INVALID_NODE, LoreErrorCode::InvalidArguments);
                Err(invalid(reason))
            };
            let internal_error = |error: StateErrors, context: &str| {
                emit_move_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                Err(MoveError::internal_with_context(error, context))
            };
            let state = internal.state();
            let repository = internal.repository_context.clone();

            if !args.node_id.is_valid_node_id() {
                return fail("node id is invalid (the root cannot be moved)");
            }
            if !args.destination_parent_id.is_valid_or_root_node_id() {
                return fail("destination parent node id is invalid");
            }

            let Ok(dst_name) = std::str::from_utf8(args.dst_name.as_bytes()) else {
                return fail("destination name is not valid UTF-8");
            };
            if dst_name.is_empty() {
                return fail("destination name is empty");
            }
            if dst_name.contains('/') || dst_name.contains('\\') {
                return fail("destination name must not contain path separators");
            }

            let Ok(node) = state.node(repository.clone(), args.node_id).await else {
                return fail("node id is unknown");
            };
            // Every real node has a non-empty name, so a zero name hash means
            // the id landed on an unallocated slot rather than a real node.
            if node.name_hash == 0 {
                return fail("node id does not resolve to a named node");
            }
            // A discarded slot keeps its name for history weaving; the node
            // itself is gone (e.g. deleted through this handle).
            if node.is_discarded() {
                return fail("node id resolves to a deleted node");
            }

            let Ok(destination) = state
                .node(repository.clone(), args.destination_parent_id)
                .await
            else {
                return fail("destination parent node id is unknown");
            };
            if destination.is_discarded() {
                return fail("destination parent id resolves to a deleted node");
            }
            if !destination.is_directory() {
                return fail("destination parent is not a directory in the handle's own tree");
            }
            if args.destination_parent_id != ROOT_NODE {
                match state
                    .node_name_clone(repository.clone(), args.destination_parent_id)
                    .await
                {
                    Ok(name) if name.is_empty() => {
                        return fail("destination parent id does not resolve to a named node");
                    }
                    Ok(_) => {}
                    Err(error) => return internal_error(error, "State::node_name_clone"),
                }
            }

            // Walk the destination's parent chain to the root: passing
            // through the moved node means the destination lives inside the
            // moved subtree, and linking there would create a cycle.
            let mut ancestor = args.destination_parent_id;
            while ancestor.is_valid_node_id() {
                if ancestor == args.node_id {
                    return fail("destination lives inside the moved subtree");
                }
                let Ok(ancestor_node) = state.node(repository.clone(), ancestor).await else {
                    return fail("destination parent chain is unreadable");
                };
                ancestor = ancestor_node.parent;
            }

            let dst_name_hash = hash_string(dst_name);
            if node.parent == args.destination_parent_id && node.name_hash == dst_name_hash {
                return fail("source and destination are the same");
            }
            match state
                .find_subnode(repository.clone(), args.destination_parent_id, dst_name_hash)
                .await
            {
                Ok(existing) if existing != args.node_id => {
                    return fail("destination name already exists under the parent");
                }
                Ok(_) => {}
                Err(error) if error.is_node_not_found() => {}
                Err(error) => return internal_error(error, "State::find_subnode"),
            }

            let block_index = NodeBlock::index(args.node_id);
            let node_index = Node::index(args.node_id);
            let block = match state.block(repository.clone(), block_index).await {
                Ok(block) => block,
                Err(error) => return internal_error(error, "read node block"),
            };
            let mut node = block.node(node_index);

            if node.parent != args.destination_parent_id {
                // Unlink from the previous parent's child chain (mirrors the
                // stage path's move surgery).
                let old_parent_id = node.parent;
                let old_parent_block_index = NodeBlock::index(old_parent_id);
                let old_parent_node_index = Node::index(old_parent_id);
                let old_parent_block = match state
                    .block(repository.clone(), old_parent_block_index)
                    .await
                {
                    Ok(block) => block,
                    Err(error) => return internal_error(error, "read old parent block"),
                };
                let old_parent = old_parent_block.node(old_parent_node_index);
                if old_parent.child == args.node_id {
                    let dirtied = {
                        let mut block_writer = old_parent_block.write();
                        block_writer.node(old_parent_node_index).child = node.sibling;
                        block_writer.mark_dirty()
                    };
                    if dirtied {
                        state.block_modified(old_parent_block, old_parent_block_index);
                        state.mark_dirty();
                    }
                } else {
                    let mut found = false;
                    let mut child_id = old_parent.child().unwrap_or_default();
                    let mut cycle = SiblingCycleGuard::new(old_parent_id);
                    while child_id.is_valid_node_id() {
                        let child = match state.node(repository.clone(), child_id).await {
                            Ok(child) => child,
                            Err(error) => return internal_error(error, "walk old sibling chain"),
                        };
                        if let Err(error) = child.walk_step(child_id, old_parent_id, &mut cycle) {
                            return internal_error(error, "walk old sibling chain");
                        }
                        let Some(sibling) = child.sibling() else {
                            break;
                        };
                        if sibling == args.node_id {
                            let child_block_index = NodeBlock::index(child_id);
                            let child_node_index = Node::index(child_id);
                            let child_block = match state
                                .block(repository.clone(), child_block_index)
                                .await
                            {
                                Ok(block) => block,
                                Err(error) => {
                                    return internal_error(error, "read sibling block");
                                }
                            };
                            let dirtied = {
                                let mut block_writer = child_block.write();
                                block_writer.node(child_node_index).sibling = node.sibling;
                                block_writer.mark_dirty()
                            };
                            if dirtied {
                                state.block_modified(child_block, child_block_index);
                                state.mark_dirty();
                            }
                            found = true;
                            break;
                        }
                        child_id = sibling;
                    }
                    if !found {
                        emit_move_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                        return Err(MoveError::internal(
                            "node not found in its parent's child chain",
                        ));
                    }
                }

                // Link into the destination parent's child chain.
                let dst_block_index = NodeBlock::index(args.destination_parent_id);
                let dst_node_index = Node::index(args.destination_parent_id);
                let dst_block = match state.block(repository.clone(), dst_block_index).await {
                    Ok(block) => block,
                    Err(error) => return internal_error(error, "read destination block"),
                };
                let sibling_node_id = dst_block.node(dst_node_index).child;
                let dirtied = {
                    let mut block_writer = dst_block.write();
                    block_writer.node(dst_node_index).child = args.node_id;
                    block_writer.mark_dirty()
                };
                if dirtied {
                    state.block_modified(dst_block, dst_block_index);
                    state.mark_dirty();
                }
                node.sibling = sibling_node_id;
            }

            node.parent = args.destination_parent_id;
            if node.name_hash != dst_name_hash {
                if let Err(error) = block.deserialize_nametable(repository.clone()).await {
                    return internal_error(error, "deserialize name table");
                }
                node.name_hash = dst_name_hash;
                let stored = {
                    let mut block_writer = block.write();
                    block_writer.node_name_store(dst_name, node.name_offset, node.name_length)
                };
                match stored {
                    Ok((name_offset, name_length)) => {
                        node.name_offset = name_offset;
                        node.name_length = name_length;
                    }
                    Err(error) => {
                        emit_move_complete(id, INVALID_NODE, LoreErrorCode::Internal);
                        return Err(MoveError::internal_with_context(
                            error,
                            "store renamed node name",
                        ));
                    }
                }
            }

            let dirtied = {
                let mut block_writer = block.write();
                *block_writer.node(node_index) = node;
                block_writer.mark_dirty()
            };
            if dirtied {
                state.block_modified(block, block_index);
                state.mark_dirty();
            }

            // A node added in this handle stays an add — its move is
            // invisible to the parent revision (mirrors the stage path).
            let move_flag = if node.is_staged_add() {
                NodeFlags::StagedAdd
            } else {
                NodeFlags::StagedMove
            };
            if let Err(error) = state
                .node_mark(repository.clone(), args.node_id, move_flag, true)
                .await
            {
                return internal_error(error, "State::node_mark");
            }
            if node.is_directory()
                && let Err(error) =
                    mark_children_moved(state.clone(), repository.clone(), args.node_id, move_flag)
                        .await
            {
                return internal_error(error, "mark children moved");
            }

            emit_move_complete(id, args.node_id, LoreErrorCode::None);
            Ok(())
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use lore_base::types::Address;
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
        MoveComplete(u64, NodeID, LoreErrorCode),
        Other(u32),
    }

    impl CapturedEvent {
        fn from_event(event: &LoreEvent) -> Self {
            match event {
                LoreEvent::Error(data) => Self::Error(data.error_type),
                LoreEvent::Complete(data) => Self::Complete(data.status),
                LoreEvent::RevisionTreeLoaded(data) => Self::RevisionTreeLoaded(data.handle_id),
                LoreEvent::RevisionTreeMoveComplete(data) => {
                    Self::MoveComplete(data.id, data.node_id, data.error_code)
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

    fn move_outcome(events: &[CapturedEvent], id: u64) -> Option<(NodeID, LoreErrorCode)> {
        events.iter().find_map(|event| match event {
            CapturedEvent::MoveComplete(event_id, node_id, error_code) if *event_id == id => {
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

    fn handle_state(handle: LoreRevisionTree) -> (Arc<State>, Arc<RepositoryContext>) {
        let entry = rt_handle::REGISTRY
            .get(&handle.handle_id)
            .expect("handle registered");
        (entry.state(), entry.repository_context.clone())
    }

    async fn add_node(handle: LoreRevisionTree, parent: NodeID, name: &str, is_file: bool) -> NodeID {
        let (state, repository_context) = handle_state(handle);
        let node = Node {
            flags: if is_file { NodeFlags::File.bits() } else { 0 },
            name_hash: hash_string(name),
            address: Address {
                hash: Hash::from([0x42u8; 32]),
                context: Context::from([0x11u8; 16]),
            },
            ..Default::default()
        };
        state
            .node_add(repository_context, parent, node, name)
            .await
            .expect("node_add must succeed")
    }

    async fn run_move(
        handle: LoreRevisionTree,
        id: u64,
        node_id: NodeID,
        destination_parent_id: NodeID,
        dst_name: &str,
    ) -> (i32, Vec<CapturedEvent>) {
        let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let status = move_node(
            LoreGlobalArgs::default(),
            LoreRevisionTreeMoveArgs {
                id,
                handle,
                node_id,
                destination_parent_id,
                dst_name: LoreString::from_str(dst_name),
            },
            make_callback(sink.clone()),
        )
        .await;
        let events = sink.lock().unwrap().clone();
        (status, events)
    }

    fn release(handle: LoreRevisionTree, store_handle_id: u64) {
        rt_handle::unregister(handle);
        storage_handle::unregister(crate::storage::handle::LoreStore {
            handle_id: store_handle_id,
        });
    }

    #[tokio::test]
    async fn move_reparents_a_file_and_preserves_its_file_id() {
        let (handle, store_handle_id) = load_handle("move-file", Partition::from([0x11u8; 16])).await;
        let dir_id = add_node(handle, ROOT_NODE, "docs", false).await;
        let file_id = add_node(handle, ROOT_NODE, "doc.md", true).await;

        let (state, repository_context) = handle_state(handle);
        let file_id_before = state
            .node(repository_context.clone(), file_id)
            .await
            .expect("node must be readable")
            .address
            .context;

        let (status, events) = run_move(handle, 1, file_id, dir_id, "renamed.md").await;

        assert_eq!(status, 0, "moving a file must succeed, got {events:?}");
        let (echoed, error_code) = move_outcome(&events, 1).expect("MoveComplete must fire");
        assert_eq!(error_code, LoreErrorCode::None);
        assert_eq!(echoed, file_id, "the moved node id must be echoed");
        assert!(events.contains(&CapturedEvent::Complete(0)));

        let node = state
            .node(repository_context.clone(), file_id)
            .await
            .expect("moved node must be readable");
        assert_eq!(node.parent, dir_id, "the node must reparent");
        assert_eq!(
            node.address.context, file_id_before,
            "the file id must be preserved across the move"
        );
        let name = state
            .node_name_clone(repository_context.clone(), file_id)
            .await
            .expect("moved node must be named");
        assert_eq!(name, "renamed.md");
        assert_eq!(
            state
                .find_subnode(repository_context.clone(), dir_id, hash_string("renamed.md"))
                .await
                .expect("the destination must resolve the new name"),
            file_id,
        );
        assert!(
            state
                .find_subnode(repository_context, ROOT_NODE, hash_string("doc.md"))
                .await
                .is_err(),
            "the old name must no longer resolve under the old parent"
        );

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn rename_within_the_same_parent_succeeds() {
        let (handle, store_handle_id) = load_handle("move-rename", Partition::from([0x22u8; 16])).await;
        let file_id = add_node(handle, ROOT_NODE, "doc.md", true).await;

        let (status, events) = run_move(handle, 2, file_id, ROOT_NODE, "renamed.md").await;

        assert_eq!(status, 0, "a rename must succeed, got {events:?}");
        let (state, repository_context) = handle_state(handle);
        let name = state
            .node_name_clone(repository_context, file_id)
            .await
            .expect("renamed node must be named");
        assert_eq!(name, "renamed.md");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn move_into_own_subtree_returns_invalid_arguments() {
        let (handle, store_handle_id) = load_handle("move-cycle", Partition::from([0x33u8; 16])).await;
        let outer = add_node(handle, ROOT_NODE, "outer", false).await;
        let inner = add_node(handle, outer, "inner", false).await;

        let (status, events) = run_move(handle, 3, outer, inner, "outer").await;
        assert_eq!(status, 1, "a move into the moved subtree must fail");
        let (_, error_code) = move_outcome(&events, 3).expect("MoveComplete must fire");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");

        let (status, events) = run_move(handle, 4, outer, outer, "self").await;
        assert_eq!(status, 1, "a move into itself must fail, got {events:?}");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn move_to_an_occupied_name_or_itself_returns_invalid_arguments() {
        let (handle, store_handle_id) =
            load_handle("move-occupied", Partition::from([0x44u8; 16])).await;
        let dir_id = add_node(handle, ROOT_NODE, "docs", false).await;
        let file_id = add_node(handle, ROOT_NODE, "doc.md", true).await;
        add_node(handle, dir_id, "taken.md", true).await;

        let (status, events) = run_move(handle, 5, file_id, dir_id, "taken.md").await;
        assert_eq!(status, 1, "an occupied destination name must fail");
        let (_, error_code) = move_outcome(&events, 5).expect("MoveComplete must fire");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");

        let (status, events) = run_move(handle, 6, file_id, ROOT_NODE, "doc.md").await;
        assert_eq!(status, 1, "a no-op move must fail, got {events:?}");

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn move_marks_the_subtree_staged_move() {
        let (handle, store_handle_id) =
            load_handle("move-marks", Partition::from([0x55u8; 16])).await;
        let dir_id = add_node(handle, ROOT_NODE, "docs", false).await;
        let child = add_node(handle, dir_id, "a.md", true).await;
        let dst_id = add_node(handle, ROOT_NODE, "dst", false).await;

        // Clear the staged bits the fixture adds left behind so the test
        // observes the move's own marking.
        let (state, repository_context) = handle_state(handle);
        for node_id in [dir_id, child, dst_id] {
            let block = state
                .block(repository_context.clone(), NodeBlock::index(node_id))
                .await
                .expect("block must load");
            let mut writer = block.write();
            writer.node(Node::index(node_id)).clear_all_change_flags();
        }

        let (status, events) = run_move(handle, 7, dir_id, dst_id, "docs").await;
        assert_eq!(status, 0, "moving a directory must succeed, got {events:?}");

        let moved = state
            .node(repository_context.clone(), dir_id)
            .await
            .expect("moved directory must be readable");
        assert!(
            moved.is_staged_move(),
            "the moved directory must be marked StagedMove, flags {:x}",
            moved.flags
        );
        let moved_child = state
            .node(repository_context, child)
            .await
            .expect("moved child must be readable");
        assert!(
            moved_child.is_staged_move(),
            "children of a moved directory must be marked StagedMove, flags {:x}",
            moved_child.flags
        );

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn move_rejects_invalid_arguments() {
        let (handle, store_handle_id) =
            load_handle("move-invalid", Partition::from([0x66u8; 16])).await;
        let file_id = add_node(handle, ROOT_NODE, "doc.md", true).await;
        let other_file = add_node(handle, ROOT_NODE, "other.md", true).await;

        // The root cannot be moved.
        let (status, _) = run_move(handle, 8, ROOT_NODE, ROOT_NODE, "root").await;
        assert_eq!(status, 1);
        // Unknown node.
        let (status, _) = run_move(handle, 9, 1_000_000, ROOT_NODE, "doc.md").await;
        assert_eq!(status, 1);
        // A file cannot be the destination parent.
        let (status, _) = run_move(handle, 10, file_id, other_file, "doc.md").await;
        assert_eq!(status, 1);
        // Names with separators are rejected.
        let (status, _) = run_move(handle, 11, file_id, ROOT_NODE, "a/b").await;
        assert_eq!(status, 1);
        // Empty names are rejected.
        let (status, _) = run_move(handle, 12, file_id, ROOT_NODE, "").await;
        assert_eq!(status, 1);

        release(handle, store_handle_id);
    }

    #[tokio::test]
    async fn move_on_unknown_handle_emits_move_complete_with_invalid_arguments() {
        let sink: Arc<Mutex<Vec<CapturedEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let status = move_node(
            LoreGlobalArgs::default(),
            LoreRevisionTreeMoveArgs {
                id: 13,
                handle: LoreRevisionTree::INVALID,
                node_id: 1,
                destination_parent_id: ROOT_NODE,
                dst_name: LoreString::from_str("doc.md"),
            },
            make_callback(sink.clone()),
        )
        .await;

        assert_eq!(status, 1, "moving against an unknown handle must fail");
        let events = sink.lock().unwrap().clone();
        let (node_id, error_code) = move_outcome(&events, 13)
            .expect("a handle miss must still emit MoveComplete carrying the caller id");
        assert_eq!(error_code, LoreErrorCode::InvalidArguments, "got {events:?}");
        assert_eq!(node_id, INVALID_NODE);
        assert!(events.contains(&CapturedEvent::Complete(1)));
    }
}
