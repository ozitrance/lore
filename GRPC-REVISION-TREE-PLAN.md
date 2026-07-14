# Running the Memory-Based Revision Workflows over the gRPC Contracts

Research notes: what the wire surface and server backends already provide,
what the new `lore_revision_tree_*` capabilities need from them, and what
still has to be built to run the no-working-tree write workflow through
gRPC. Companion to `PROGRESS.md` (the SDK-side implementation) and
`docs/proposals/2026-05-14-low-level-revision-api.md` (which explicitly
scoped gRPC out and anticipated this follow-on: *"The proposal does not
preclude exposing a subset of these operations as RPCs later for genuinely
remote callers; that is a separate LEP"*).

---

## 1. The protocol landscape today

There are **two generations** of wire contracts, both served simultaneously
(`lore-server/src/grpc/server.rs` registers every service below):

### 1a. Legacy `urc.*` services

- Protos: `lore-server/src/legacy/proto/{storage,revision,repository,environment}.proto`
  (packages `urc.rpc` / `urc.model`; generated into
  `lore-server/src/legacy/generated/` and `lore-proto/src/grpc/urc.*`).
- This is what the **SDK/CLI talks to today**: `lore-transport` defines the
  `Storage` / `Revision` / `Repository` / … traits (`lore-transport/src/traits.rs`)
  with both gRPC (`lore-transport/src/grpc/`) and QUIC implementations. The
  storage handle's remote (`StoreInternal::remote` →
  `endpoint.session_connection(partition)`) hands out exactly these traits.
- Server handlers are shared with v1 where possible via
  `lore-server/src/grpc/handlers/*` (e.g. `branch_push.rs` backs both
  generations).

### 1b. `lore.*.v1` services (the go-forward contracts)

Protos in `lore-proto/proto/lore/`, implementations in
`lore-server/src/grpc/<service>/v1/`:

| Service | RPCs | Notes |
|---|---|---|
| `lore.storage.v1.StorageService` | `Get`, `GetMetadata`, `Put`, `Query`, `PresignDownload`, `Verify`, `Copy`, `MutableLoad`, `MutableStore`, `MutableCompareAndSwap` | Bidirectional streams for bulk transfer. `Put` **validates the payload hash server-side** (`protocol/storage/put.rs::validate_hash` via `lore_storage::hash_fragment`). Repository id travels as gRPC metadata (`REPOSITORY_ID_KEY`); identity via JWT interceptor. |
| `lore.revision.v1.RevisionService` | `BranchCreate/Delete/Get/List/Push`, `BranchMetadataGet/Set`, `RevisionList` | Charter (proto header): *"minimal, stable set of graph primitives."* `BranchPush` is the **only revision-graph write**: takes a revision signature already present in CAS, deserializes the state server-side, checks `parent_self` against the current tip (tip-collision → `FailedPrecondition` with the current latest embedded), verifies referenced fragments exist (`verify_fragments`), honors branch protection (service accounts bypass), runs pre/post hooks, emits notifications, and supports `force` / `fast_forward_merge`. |
| `lore.thin_client.v1.ThinClientService` | `RevisionInfo`, `RevisionTree`, `RevisionDiff`, `ContentDiff` | Charter: *"presentation helpers for clients that lack local cache or compute (web UIs)."* **Read-only** today. Handlers build a pathless `RepositoryContext::new_server_context(...)` and walk `State` server-side — the very same machinery the revision_tree SDK surface uses, proving the server can host this logic. `RevisionTree` streams `TreeNode {path, node_type, address, last_changed_revision}` — the wire twin of `lore_revision_tree_list_children`. |
| `lore.repository.v1.RepositoryService` | `RepositoryCreate/Delete/Get/List`, `RepositoryMetadataGet/Set` | Repo lifecycle incl. default-branch creation. Caller pre-generates ids (UUIDv7) for idempotent retries. |
| `lore.environment.v1`, lock, notification, admin | — | Not relevant to this workflow. |

### 1c. The new SDK surface (recap)

`lore_revision_tree_*` (this fork, see `PROGRESS.md`): load / resolve_path /
list_children / node_info / node_path / info / add / modify / delete / move /
metadata_set / metadata_get / commit / close, all against a storage handle,
no working tree. The commit pipeline (`lore_revision::commit::commit_tree`)
**already lives in `lore-revision`** — reachable from `lore-server`. The
per-verb edit logic (validation + node surgery + delta recording) currently
lives in the `lore` crate's verb bodies (`lore/src/revision_tree/*.rs`).

**Dependency fact that shapes everything below:** `lore-server` depends on
`lore-revision`, `lore-storage`, `lore-transport`, `lore-proto` — **not** on
the `lore` crate. Server handlers can call `commit_tree` today but cannot
call the `lore` crate's revision_tree verbs.

---

## 2. What already works end-to-end (thick-client path)

A service that embeds the SDK (in-process `lore` crate) can run the full
no-clone write workflow against a remote server **today**, using only
existing wire contracts:

```
1. lore_storage_open            (remote endpoint configured)
2. lore_storage_put             → bytes land locally; content address returned
3. lore_revision_tree_load      (store, repository, base revision)
4. add / modify / delete / move / metadata_set  (in memory)
5. lore_revision_tree_commit    remote_write=1
     → tree blocks, name tables, delta block, metadata fragment, file
       fragments, and the 320-byte revision record are uploaded to the
       server CAS through the existing storage protocol (immutable::write
       with remote_write), and the *local* branch tip advances
6. urc.rpc / lore.revision.v1 BranchPush(branch_id, new_revision_signature)
     → server re-validates (tip CAS, fragment verification, protection,
       hooks) and advances the authoritative tip
```

Step 6 is the one seam that is not yet wrapped by the SDK surface: the
existing `lore branch push` verb is working-tree-based, and the transport's
`Revision::branch_push` is not yet called from any revision_tree verb. See
gap **G1**.

The server-side validation story required by the LEP ("commit through this
surface honors the same branch semantics") holds on this path because
`BranchPush` is the same gate the file-system flow goes through.

---

## 3. What a pure thin client (no SDK) can and cannot do

Can, today:
- **Read everything** the workflows need: `ThinClientService.RevisionTree`
  (tree by revision, path-prefix + depth filters), `RevisionInfo`,
  `RevisionDiff`, `ContentDiff`, `StorageService.Get/GetMetadata` (bytes by
  address), `RevisionService.RevisionList` / `BranchGet` (by name!) /
  `BranchList`.
- **Upload bytes**: `StorageService.Put` (hash-validated). Caveat: the
  client must produce the fragment framing (`Fragment{flags, size_payload,
  size_content}` + payload matching `hash_fragment`) — see **Q2**.
- **Advance a tip to an existing revision**: `BranchPush`.

Cannot, today:
- **Construct a revision.** There is no RPC that turns "base revision +
  a set of path-level operations + already-uploaded content addresses"
  into a new revision. The merkle tree blocks, name tables, delta block,
  history weave, and the revision record can only be produced by the SDK.
  This is *the* gap between the wire surface and the new workflows.

---

## 4. Gaps and required work

### G1 — SDK: remote tip advance from the revision_tree surface (small)

`lore_revision_tree_commit(remote_write=1)` uploads all revision data but
advances only the local tip. For a pipeline that targets a server, add the
follow-through: after a successful commit with `remote_write`, call the
remote's `Revision::branch_push` (available on the storage handle's
connection) and surface the server's accept/reject on the commit terminal
event — the plumbing mirrors what the FS `branch push` verb does. Design
choice: fold into `commit` options (e.g. `remote_write` implies push, or a
separate `remote_push` flag) — the LEP's remote-write contract language
supports either.

### G2 — Server: a revision-construction RPC (the main work)

A pure gRPC client needs the server to run the construct-and-commit logic on
its behalf. Recommended shape (matches the sketch in `TaskInsights.md`):

**One atomic "apply changeset" RPC** rather than stateful 1:1 handle verbs.
The LEP already rejected RPC-per-edit ("would pay an RPC round-trip per
edit"), and a stateful server-side handle adds session lifetime problems
for no benefit — a thin client's edit batch is known up front.

```proto
// strawman
rpc RevisionCreate (RevisionCreateRequest) returns (RevisionCreateResponse);

message RevisionCreateRequest {
  bytes branch_id = 1;              // target branch
  bytes base_revision_signature = 2; // client's view of the tip (0 = initial)
  string commit_message = 3;
  repeated lore.thin_client.v1.Metadata metadata = 4;
  repeated Operation operations = 5;

  message Operation {
    oneof op {
      Put put = 1;        // path, kind, mode, size, address (from StorageService.Put)
      Delete delete = 2;  // path
      Move move = 3;      // from_path, to_path
    }
  }
}
message RevisionCreateResponse {
  bytes revision_signature = 1;
  uint64 revision_number = 2;
}
```

Semantics: path-keyed (thin clients think in paths; the server resolves to
node ids internally), all-or-nothing, tip-collision against
`base_revision_signature` → `FailedPrecondition` embedding the current
latest (byte-for-byte the `BranchPush` contract), then the same
protection/hook/notification pipeline as `BranchPush`.

Server handler skeleton (all ingredients exist):
`RepositoryContext::new_server_context` + server write token (the pattern in
`grpc/handlers/branch_push.rs` and its tests) → `State::deserialize(base)` →
apply operations → `lore_revision::commit::commit_tree(...)` → reuse
`handlers/branch_push::push()` for the authoritative tip advance (or advance
directly, since commit_tree + the base check already performed the CAS
under the server's storage locks — decide in design).

### G3 — Code movement: extract the edit primitives into `lore-revision`

Because `lore-server` cannot depend on the `lore` crate, the verb-level edit
logic the G2 handler needs (validated add/modify/delete/move with staged
marking, discard, and delta recording — currently inline in
`lore/src/revision_tree/{add,modify,delete,move_node}.rs`) should move into
a `lore-revision` module (e.g. `lore_revision::revision_tree`), leaving the
`lore` crate verbs as thin wrappers that do event emission and handle
plumbing. `commit_tree` already made this move; this completes it. Benefit:
one implementation of the malformed-edit rejections (duplicate names,
cycles, discarded slots) shared by SDK and server — the LEP's "server
validates pushed revisions" story stays single-sourced.

### G4 — Service placement decision

Where should `RevisionCreate` live?

| Option | For | Against |
|---|---|---|
| `lore.revision.v1.RevisionService` (**recommended**) | It is a revision-graph *write*, and this service already owns the only other graph write (`BranchPush`) with all the auth/protection/hook plumbing; the charter "graph primitives" fits a commit primitive. | Grows the "minimal, stable" service. |
| `lore.thin_client.v1.ThinClientService` | Charter is literally "compute on behalf of clients that lack it", which describes server-side tree construction. | Service is read-only today; mixing presentation reads with an authoritative write muddies both charters. |
| New `lore.revision_tree.v1` service | Clean slate, mirrors the SDK namespace. | A one-RPC service; more registration/interceptor surface for little gain — can still split later, additively. |

Recommendation: `RevisionService.RevisionCreate`, keeping thin_client
read-only. Revisit only if the operation list grows presentation-flavored
options.

### G5 — Repository bootstrap for the no-clone flow

Creating a repo + default branch remotely already works
(`RepositoryService.RepositoryCreate`), and `BranchGet(name)` resolves
branch names to ids. Nothing to build; just confirm the web layer uses ids
after one resolve.

### G6 — Tests and contract docs

- Server: handler tests mirroring `branch_push.rs`'s suite (tip collision,
  protection, service-account bypass, hooks, idempotent retry, deleted
  branch), plus construct-specific cases (duplicate path ops, unknown base,
  address not in CAS → the `verify_fragments` path, empty operation list).
- End-to-end: an integration test driving `StorageService.Put` →
  `RevisionCreate` → `ThinClientService.RevisionTree` /
  `RevisionService.RevisionList` to verify the committed tree round-trips.
- Proto docs: the new RPC needs the same in-proto contract prose style as
  `BranchPush` (soft-rejection semantics, idempotency).

---

## 5. Open questions to settle in the RPC design

- **Q1 — File ids for new files.** The SDK add verb takes the caller's
  `address.context` verbatim. For thin clients, decide: require a
  client-generated UUIDv7 context (matches the storage.Put they already
  did), or let the server mint one when zero (the `commit_file` precedent).
  Leaning: require it — the address must match what was Put.
- **Q2 — Fragment framing for thin uploaders.** `StorageService.Put`
  expects `hash_fragment(fragment, payload)`-consistent framing; a browser
  client would prefer "raw bytes, flags=0". Document the minimal raw-frame
  recipe, and consider whether `RevisionCreate` should optionally accept
  small inline payloads to spare tiny edits the CAS round-trip (ergonomics
  vs. keeping structure/bytes separation — the LEP's split argues for
  keeping them separate).
- **Q3 — Tip advance inside RevisionCreate vs. explicit BranchPush.**
  Atomic construct+advance (one RPC, matches TaskInsights' web-API sketch)
  vs. construct-only returning the signature and letting the client call
  `BranchPush` (composability, reuses the exact existing gate). Leaning:
  atomic, with the handler internally reusing the `push()` helper — a web
  endpoint wants one conflict-checked call.
- **Q4 — Operation vocabulary v1.** `put/delete/move` covers the importer,
  mirror, and build-ingest use cases. Explicit `mkdir` (empty directory)
  and link operations can be additive later; the SDK supports them if we
  choose to expose them day one.
- **Q5 — Limits.** Max operations per request, max metadata entries, and
  whether a very large changeset should become a client-streamed request
  (`stream RevisionCreateRequest` with a terminating commit message) —
  server memory for the in-flight State scales with edits (LEP's stated
  risk), so a cap + documented batching guidance is probably enough for v1.

---

## 6. Suggested order of work

1. **G3** — move the tree-edit primitives into `lore-revision` (pure
   refactor, keeps SDK tests green; unblocks the server handler).
2. **G2 + G4** — proto for `RevisionService.RevisionCreate`, server handler
   on the branch_push plumbing, hash/fragment validation via existing
   paths.
3. **G1** — SDK follow-through: remote tip advance from
   `lore_revision_tree_commit` (independent of 1–2; do in parallel if the
   thick-client path is wanted sooner).
4. **G6** — handler + end-to-end tests, proto contract docs.
5. Revisit **Q2/Q5** ergonomics once a real web client exercises the flow.
