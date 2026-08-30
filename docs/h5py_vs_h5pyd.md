# h5py vs h5pyd: Functional Differences

h5pyd is a client for [HSDS](https://github.com/HDFGroup/hsds) (the HDF Scalable
Data Service), a REST server that implements the HDF5 data model. h5pyd's
high-level API mirrors h5py's as closely as possible, but the underlying
transport — HTTP requests to a multi-tenant server instead of local calls into
libhdf5 — creates real, permanent differences. This document is a snapshot of
where the two diverge: features unique to each, extra parameters h5pyd adds to
familiar h5py calls, and HSDS-specific concepts with no local-file equivalent.

It is not a list of bugs to fix — most of what's below is either an
intentional consequence of the client/server architecture or a deliberate
extension for that architecture. A few genuinely-open gaps are called out
explicitly in [Known Gaps](#known-gaps-not-architectural).

## Contents

- [New classes h5pyd adds](#new-classes-h5pyd-adds)
- [`File()` — extra parameters](#file--extra-parameters)
- [`create_dataset` / `create_group` / `create_table` — extra parameters](#create_dataset--create_group--create_table--extra-parameters)
- [HSDS-specific concepts](#hsds-specific-concepts)
- [Behavioral differences](#behavioral-differences)
- [h5py features not available in h5pyd](#h5py-features-not-available-in-h5pyd)
- [h5pyd features not available in h5py](#h5pyd-features-not-available-in-h5py)
- [Known gaps (not architectural)](#known-gaps-not-architectural)

---

## New classes h5pyd adds

These have no h5py equivalent at all.

### `Folder` (`h5pyd/_hl/folders.py`)

Represents an HSDS *folder* — a node in the server-side namespace that domains
(HSDS's equivalent of files) live in. Because HSDS domains aren't organized on
a local filesystem, `Folder` is how you browse and manage that namespace:
list subdomains, filter them by name pattern or by a root-attribute query,
and manage per-domain ACLs.

```python
f = h5pyd.Folder("/home/jreadey/", endpoint=..., username=..., password=...)
for name in f:
    print(name, f[name].is_folder)
```

- Constructor: `domain_name, pattern=None, query=None, mode=None, endpoint=None, username=None, password=None, bucket=None, api_key=None, owner=None, batch_size=1000, retries=3, logger=None, verbose=False`
- Properties: `domain`, `parent`, `modified`, `created`, `owner`, `is_folder`
- Methods: `getACL(username)`, `getACLs()`, `putACL(acl)`, `delete_item(name, keep_root=False)`, plus `__getitem__`/`__delitem__`/`__len__`/`__iter__`/`__contains__`/context-manager support

### `MultiManager` (`h5pyd/_hl/dataset.py`)

Batches reads/writes across several datasets in one call, dispatched
concurrently (a thread pool, default up to 16 workers, optionally spread
across multiple HSDS service-node ports via `SN_CORES`/`SN_PORT_RANGE`). Loosely
analogous to HDF5 1.14's `H5Dread_multi`/`H5Dwrite_multi`, but implemented
entirely client-side for parallel HTTP requests rather than a single C-level
batched call.

```python
mm = h5pyd.MultiManager(datasets=[ds1, ds2, ds3])
mm[0:10]                  # same selection applied to every dataset
mm[[sel1, sel2, sel3]]    # per-dataset selections
```

### `Table` and `Cursor` (`h5pyd/_hl/table.py`)

A PyTables-style query interface over a 1-D compound-dtype dataset —
server-side filtering/updating instead of pulling everything and filtering in
numpy. (h5py itself has no built-in row-query engine; the closest analog in
the broader ecosystem is the separate `PyTables`/`tables` package, not h5py.)

- `Group.create_table(name, numrows=None, dtype=None, data=None, **kwds)` creates one (returns a `Table`, a `Dataset` subclass, with `maxshape=(0,)` forced so it can grow).
- `Table`: `colnames`, `nrows`, `read(start=0, stop=None, field=None, out=None)`, `read_where(condition, field=None, ...)`, `update_where(condition, value, start=0, stop=None, limit=0)`, `get_where_list(condition, start=0, stop=None, limit=0)`, `append(rows)`.
- `Cursor(table, query=None, start=None, stop=None, limit=0, field=None, condvars=None, buffer_rows=None)` — a lazy row iterator with server-side query pushdown.

### `ACL` (`h5pyd/_hl/base.py`)

A plain data holder for one access-control-list entry: `username` plus
boolean `create`/`delete`/`read`/`update`/`readACL`/`updateACL` flags. See
[HSDS-specific concepts](#hsds-specific-concepts) below for where ACLs are
actually read/written.

`getACL(username)`/`getACLs()`/`putACL(acl)` are available on both `File`
and `Folder` (backed by a shared `ACLManager` helper in
`h5pyd/_hl/acl_manager.py` that both delegate to), so the `hsacl` CLI works
against file domains as well as folders.

### `PointsAccessor` (`h5pyd/_hl/dataset.py`, exposed as `Dataset.points`)

An explicit accessor for point selections: `dset.points[[(1,3),(5,1),(2,7)]]`
for both get and set, in one round trip regardless of ordering or repeated
points. h5py has no separate accessor for this — it handles point selection
through ordinary fancy-indexing on `__getitem__`/`__setitem__` (with
narrower validation; see [Behavioral differences](#behavioral-differences)).

### Not new, despite looking unfamiliar

`AstypeWrapper`, `AsStrWrapper`, `FieldsWrapper`, `Empty`, `HardLink`,
`SoftLink`, `ExternalLink`, `DimensionProxy`/`DimensionManager` all exist in
h5py too, playing the same role. The one genuine addition in that group is
`UserDefinedLink` (`group.py`), which has no h5py counterpart.

---

## `File()` — extra parameters

h5pyd's `File` accepts h5py's `mode`, `libver`, `swmr`, and `track_order`,
but two of those behave differently (see table), and it adds a set of
connection/auth parameters h5py has no reason to need:

| Parameter | Purpose |
|---|---|
| `domain` (replaces h5py's `name`) | HSDS domain path/URI, e.g. `/home/user/tall.h5` or DNS-style `tall.username.home` |
| `endpoint` | HSDS server URL (default `http://localhost:5101`) |
| `username` / `password` | HSDS auth credentials |
| `api_key` | alternate auth for API-key-based server configs |
| `bucket` | storage bucket/container backing the domain |
| `owner` | owner to assign to a newly-created domain (admin-only) |
| `retries` (default 10) | retry attempts on failed server requests |
| `timeout` (default 180) | request timeout, in seconds |
| `logger` | custom log handler |
| `use_session` (`**kwds`) | keep the HTTP connection alive between calls |
| `use_cache` (`**kwds`) | cache attribute/link values instead of re-fetching on every access |
| `linked_domain` (`**kwds`) | create the new domain's root by linking to another domain's root |

Two h5py-compatible parameters are accepted but don't mean the same thing:

- **`libver`** is a no-op — it's accepted and echoed back as `("0.0.1", "0.0.1")`, but doesn't select a format version the way it does in h5py.
- **`swmr`** doesn't engage HDF5's actual single-writer/multi-reader file format feature (there's no local file to apply it to). Setting `swmr=True` instead forces h5pyd to bypass its local metadata cache so every read hits the server fresh — useful for the same "don't read stale state" goal SWMR serves in h5py, but a different mechanism.

---

## `create_dataset` / `create_group` / `create_table` — extra parameters

`Group.create_dataset()` supports the same core h5py kwargs (`chunks`,
`maxshape`, `compression`, `compression_opts`, `scaleoffset`, `shuffle`,
`fletcher32`, `fillvalue`, `track_order`, `track_times`) plus two additions
for HSDS's server-side chunk initialization:

| Parameter | Purpose |
|---|---|
| `initializer` | name of a server-side chunk-initializer function to run when a chunk is first allocated |
| `initializer_args` | arguments passed to that initializer |

`Group.create_group()` and `Group.get()` both accept `track_order` per-call
in h5pyd; in h5py, creation-order tracking is fixed at file-creation time and
isn't a per-call `create_group`/`get` option.

`Group.create_table(name, numrows=None, dtype=None, data=None, **kwds)` has
no h5py equivalent at all (see [`Table`](#new-classes-h5pyd-adds) above).

---

## HSDS-specific concepts

Concepts that only make sense because there's a multi-tenant server behind
h5pyd, with no local-file analog:

- **Domains** — HSDS's file-equivalent, but organized in a server-side folder
  tree rather than a filesystem path. This is what `File`'s `domain=`
  argument names, and what `Folder` browses.
- **ACLs (access control lists)** — per-user `create`/`delete`/`read`/`update`/
  `readACL`/`updateACL` permissions on a domain or folder. h5py has no access
  control of its own; local files rely on OS filesystem permissions instead.
  Managed via `File`/`Folder`'s `getACL()`/`getACLs()`/`putACL()`, and the
  `hsacl` CLI.
- **Buckets** — S3-style storage-backend/container selection (`bucket=` on
  `File`, `Folder`, `getServerInfo`) — no analog for a local file.
- **`owner`** — multi-tenant ownership of a domain (`File.owner` property,
  `owner=` constructor kwarg, admin-settable).
- **`linked_domain`** — create a new domain whose root group links to another
  domain's root, a zero-copy-ish sharing mechanism specific to HSDS's
  object-store-backed model.
- **`getServerInfo()`** (`h5pyd/serverinfo.py`) — hits HSDS's `/about`
  endpoint for server version/build info; nothing to ask when there's no
  server.
- **Server-side storage stats** — `File` exposes `num_objects`,
  `num_datatypes`, `num_groups`, `num_chunks`, `num_linked_chunks`,
  `num_datasets`, `allocated_bytes`, `metadata_bytes`, `linked_bytes`,
  `total_size`, `md5_sum` as properties; `Dataset` exposes `num_chunks` and
  `allocated_size`. h5py has no server to ask for storage/usage stats.
- **`retries` / `timeout`** — HTTP-transport resilience knobs, meaningless
  for h5py's direct libhdf5 calls.
- **Object lookup by UUID** — `f["datasets/{uuid}"]` / `f["groups/{uuid}"]`
  fetches an object directly by its server-side id; h5py has no UUID concept
  to look up by.
- **`obj.modified`** — datetime property exposing HSDS's tracked
  last-modified time for an object; no h5py equivalent.

---

## Behavioral differences

Same feature on both sides, but it behaves differently:

- **Fancy-indexing validation is relaxed in h5pyd.** h5py requires index
  lists passed to `__getitem__` to be strictly increasing and non-repeating,
  raising `TypeError` otherwise. h5pyd doesn't enforce this, and (per the
  `PointsAccessor`/multi-fancy-index work this session) genuinely supports
  more than one list/array index at once and repeated/out-of-order point
  values — where h5py raises `TypeError` ("multiple indexing vectors not
  allowed").
- **`track_times=False` is ignored by HSDS** — it always tracks
  modification time regardless of this h5py dataset-creation flag.
- **Chunk layout is always chunked under HSDS**, even when a dataset would be
  "contiguous" under h5py — `dset.chunks` is never `None` for an HSDS-backed
  dataset.
- **Compression/filters:**
  - `lzf` is unsupported by HSDS; HSDS uses `lz4` instead.
  - HSDS/h5pyd additionally supports `lz4hc` and `blosclz`
    (`H5Z_FILTER_BLOSC`) compression, neither of which h5py supports.
    `zstd` is listed in `File.compressors` too, but creating a dataset with
    `compression="zstd"` currently fails server-side
    (`H5Z_FILTER_ZSTD not supported`). Also note that unlike `lz4`, HSDS
    doesn't report back the compression level (`compression_opts`) for
    `blosclz`/`lz4hc` after creation, even though it was accepted at
    creation time.
  - `scaleoffset` is currently a no-op under HSDS — data round-trips
    losslessly instead of applying the expected lossy transform (open TBD in
    the HSDS code).
  - HSDS doesn't raise `IndexError` for a missing `compression_opts` on gzip,
    where h5py does.
  - Compression backend enumeration differs: HSDS exposes `File.compressors`;
    h5py exposes `h5py.filters.encode`.
- **Default fill value for vlen string datasets** differs: h5py defaults to
  `b""`, h5pyd defaults to `0` (an internal "unset" placeholder).
- **Object-id equality.** h5pyd object ids (`.id.id`) are stable, comparable
  strings (HSDS UUIDs) — two independently-obtained handles to the same
  object compare equal. h5py's low-level ids aren't guaranteed to compare
  this way.
- **Complex numbers.** h5pyd only supports `complex64`/`complex128`;
  `complex256` (long double) raises `TypeError`, where h5py supports it.

---

## h5py features not available in h5pyd

- **Low-level API.** h5py exposes Cython bindings for the raw HDF5 C API
  (`h5py.h5f`, `h5d`, `h5t`, `h5s`, `h5a`, `h5r`, `h5p`, `h5g`, ...). h5pyd
  has none of these — it's a REST client with no local libhdf5 to bind to.
  (`h5pyd/h5ds.py` exists for dimension-scale helpers but is unrelated to
  h5py's low-level `h5d` module.)
- **Virtual datasets (VDS).** No `VirtualSource`, `VirtualLayout`, or
  `create_virtual_dataset` — This feature is not yet supported in HSDS.
- **File drivers.** h5py's `driver=` kwarg (`core`, `family`, `fileobj`,
  `mpio`, ...) has no equivalent — `File.driver` is a hardcoded read-only
  `"rest_driver"` string, and there's no constructor parameter to change it.
- **Direct raw-chunk I/O.** h5py's `Dataset.id.read_direct_chunk`/
  `write_direct_chunk` (bypassing filters to move compressed bytes directly)
  has no h5pyd equivalent. (h5pyd's `read_direct`/`write_direct` exist, but
  those are the *ordinary* h5py methods for skipping fancy-indexing overhead
  on full-array transfers — not raw chunk access.)
- **MPI/parallel HDF5.** h5py's `mpi4py`-based collective I/O has no
  equivalent — HSDS's concurrency model is independent REST requests, not
  MPI collectives.
- **Real SWMR semantics.** As noted above, `swmr=` is accepted but repurposed
  to mean "bypass the local cache," not HDF5's actual SWMR file format
  feature.

---

## h5pyd features not available in h5py

- **`Folder`, `MultiManager`, `Table`/`Cursor`, `ACL`, `PointsAccessor`** —
  see [New classes h5pyd adds](#new-classes-h5pyd-adds).
- **`Dataset.query()` / field-restricted server-side query with optional
  in-place update** (`query(query, selection=None, limit=0, update_value=None)`) —
  push a boolean-expression filter to the server and optionally update
  matching elements, without pulling the whole dataset locally.
- **`Dataset.num_chunks` / `Dataset.allocated_size`, and `File`'s storage
  stats** — see [HSDS-specific concepts](#hsds-specific-concepts).
- **Multiple simultaneous fancy-index vectors and unordered/repeated point
  selections** — see [Behavioral differences](#behavioral-differences).
- **`track_order` as a per-call argument** on `create_group`/`get`, rather
  than a file-level setting.
- **UUID-based direct object lookup** (`f["datasets/{uuid}"]`).

---

## Known gaps (not architectural)

Two open, genuinely-unintended h5pyd bugs, worth calling out separately
because they're not consequences of the client/server design — they're just
unfinished:

- **`Dataset.astype(dtype)` doesn't actually convert on read.**
  `dset.astype('f4')[:]` is supposed to return data cast to `'f4'`, matching
  h5py, but currently returns data in the dataset's original dtype — the
  cast is computed internally but never applied before the array is
  returned from `Dataset.__getitem__`.
- **`Dataset.__eq__`/`__ne__` are not symmetric against a bare numpy
  scalar/array.** `val == dset` and `dset == val` can disagree. This mirrors
  an old h5py bug (#1947) that upstream h5py has since fixed but h5pyd
  hasn't picked up — comparison for `HLObject` (`File`/`Group`/`Dataset`) is
  identity-based, which is fine when comparing two `HLObject`s but produces
  the asymmetry when compared against a plain value.
- **Field-restricted `query()` isn't supported.** Passing both a query
  string and a `fields=` selection to `Dataset.__getitem__` raises
  `IOError("field selection not supported with query")` — a hard
  limitation, not a bug per se, but worth listing since it's a plausible
  thing to reach for and currently unavailable.
