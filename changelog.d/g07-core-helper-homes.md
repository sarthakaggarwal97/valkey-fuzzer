### Changed

- One home per helper the core had copied: `_data.sha256_text` and `_data.sha256_json` digest text and canonical
  JSON for the failure signature key, the experiment, catalog and plan digests, a shortened report value, the
  owner-tagged chain and cgroup names, a provisioner directory name and the two compare digests; the local-binary
  provider digests its file through `_data.file_sha256` as the module provider does. `_data.derived_int` derives
  the component seeds, the generator's streams and the pool draws (every seed and draw is unchanged).
  `reports.read_yaml_mapping` reads a scenario, a zoo case or a batch's drawn scenario, so a missing or unreadable
  one is an `InputError` naming the file instead of a traceback; `clock.utc_iso` stamps `generated_at`,
  `created_at` and `updated_at`; the two `--artifact NAME=PATH` checks share one function. A matrix case the
  compiler rejects with any `ValueError` is listed as invalid, as the generator and the minimizer already treat it.
