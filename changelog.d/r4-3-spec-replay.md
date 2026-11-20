### Changed

- `spec conform` replays a run's records without deep-copying the observed state twice per record: a step
  reports whether each write changed a value, and a function variable is replaced rather than written into, so
  an appended state shares what the steps after it leave alone. Same states, steps, `sample_mismatches` and
  `conform.json`; 4x faster (0.27 s to 0.065 s per 1,024 records, 1.11 s to 0.27 s per 4,096), in every batch
  worker's after-run step.
- One `derive_constants` in `spec/bundle.py` derives the model constants from a run for both `spec conform` (the
  trace header's node configs) and `valkey.oracle.cluster-election-safety` (live `CONFIG GET`), with one rule
  for a `config` constant: `TRUE` when every node's setting is known and equals, `FALSE` once one differs, and
  unknown while a node's setting is missing and none differs. `spec conform` errors on an unknown constant
  naming the node; the oracle leaves the finding unjudged (`ERROR`) naming the constant and the node. Before,
  conform took the nodes that had the setting as all of them.
- `load_bundle` takes the bundle's claims from the same cache as `load_spec_claims`.
- The election oracle reads every node's settings in one `CONFIG GET`.
