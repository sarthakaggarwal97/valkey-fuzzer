### Changed

- One home per helper the core had copied: `_data.sha256_text` and `_data.sha256_json` digest text and canonical
  JSON for the failure signature key, the experiment, catalog and plan digests, a shortened report value, the
  owner-tagged chain and cgroup names, a provisioner directory name, a spec bundle's mapping and the two compare
  digests; the local-binary provider and the TLA+ tools digest their file through `_data.file_sha256`, and
  `_data.file_stamp` is the modification-time-and-size cache key of the source digest, the spec bundle loaders,
  the TLC probe and the server config probe. `_data.derived_int` derives the component seeds, the generator's
  streams and the pool draws (every seed and draw is unchanged). `reports.read_yaml_mapping` reads a scenario, a
  zoo case, a batch spec or a batch's drawn scenario, and `reports.load_document` every JSON file a batch reads
  back, so a missing or unreadable one is named instead of a traceback (a batch's own bad file still exits 2);
  `clock.utc_iso` stamps `generated_at`, `created_at`, `updated_at` and a spec check's `started_at`; the two
  `--artifact NAME=PATH` checks share one function; the batch's conformance plug-in uses `spec.conform`'s
  `bundle_approved`. A matrix case the compiler rejects with any `ValueError` is listed as invalid, as the generator
  and the minimizer already treat it.
- A run's coverage key is the digest of the canonical JSON of its tuple, as every other digest is; the key of a
  given combination differs from what earlier versions computed. Keys are only compared within one batch.
- An exclusion, a `reject` rule or a search-profile weight whose listed value is a list, a mapping or null now
  matches an equal plan value (`fields.json_equal`, which the config schema's `choices` already used); before,
  only scalars matched.
