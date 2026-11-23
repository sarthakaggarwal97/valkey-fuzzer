### Changed

- Readability of four long functions, with no change in what they return, log, or write (the tests pin every
  summary, diagnostics record, and check result). `valkey.action.commands`' compared run derives the violating
  replies once (`_judged`: the recorded indexes, the FAIL text, and the disputed items `compared_verdict`
  takes) instead of walking the replies four times; `check_slot_coverage` returns at the first error it finds
  instead of carrying a `success` flag through four gated blocks; `DeploymentStateCheck._validate_once` keeps
  one attempt's bookkeeping in an `_Attempt` and runs its checks and its log check through `_run_checks` and
  `_check_logs`, without a `nonlocal` closure or a `for … else`; `generator.py` opens with its first paragraph
  and a pointer to `docs/scenarios-and-experiments.md` and `docs/cli.md`, whose rule table now says a range
  rule's optional `default` is drawn as one more edge.
