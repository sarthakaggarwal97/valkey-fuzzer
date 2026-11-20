### Changed

- One verdict ladder for a compared run: `valkey.action.commands` and `valkey.action.tools` both end in
  `command_sequence.compared_verdict`, which decides `expect-disputed`, `unexplained-difference`,
  `unreached-change` and PASS in that order with each action's own wording, and both resolve the two builds
  through `two_builds`. The tools action takes `without_index` and the bytes of a text-or-`!!binary` value
  (`argument_bytes`) from the sequence module instead of copies. Every summary, `diagnostics["reason"]` and
  diagnostics record is byte-identical; the tests pin them.
- The concurrency check's clients are `Side.client_view()`: the side's own class with connections of its own and
  nothing masked, instead of a forwarding `_Worker` class. `checks.replicate` and `checks.converged` are module
  functions, and the command-metadata oracle makes its replica through `replicate` (waiting for the link and the
  offset) instead of its own REPLICAOF-and-poll.
