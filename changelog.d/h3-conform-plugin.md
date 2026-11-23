### Removed: the batch worker's own spec conform check (`conform.json`)

- The batch's conform plug-in took the run's own `conform` section from its report, or, for a report without
  one, had the worker run TLC after the run and write `conform.json` into the run folder. Every run has written
  the section since 4870dd9 (PR-P2-b, the same day the worker's path was kept; no release between, and
  `format_version` is still 1), so the worker's path, `conform.json`, and the batch's reading of it are gone. A
  run that reached its verdict and whose plan names a bundle but whose report has no `conform` section (its
  worker was stopped during the check) is an `ERROR` for an approved bundle with the reason `report has no
  conform section`, and reported only for a draft; the result's `conformance` entry carries the same. A pruned
  passing run keeps its `report.json` and `scenario.yaml` alone, as before (the report carries the check).
- `BatchPlugin.after_run`, `after_run_timeout_s`, and `kept_when_pruned`, which only the conform plug-in
  implemented, leave the plug-in protocol (`batch/hooks.py`); a plug-in takes part when a job is judged and
  recorded, in the summary, and in what the command prints. The worker's deadline still covers the run's own
  check: the pool adds the batch's `conform_tlc_timeout_s` to it directly. The worker still prints one
  `spec conform` line per bundle of the run's own results.
- `spec conform` over a pruned run whose trace is gone now says `a pruned run keeps only its report` where it
  said `a pruned run keeps its conform.json`.
