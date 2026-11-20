### Changed

- The spec tools' probe (`java -cp tla2tools.jar tlc2.TLC -h`, a JVM start, and the jar's SHA-256) runs once per
  jar in a process, cached on the jar's path, size and modification time; `TlaTools` carries the `digest` it
  took, and `identity()` reads it instead of hashing the jar again. A warm `TlaTools.locate` falls from 93 ms to
  0.1 ms and `identity()` from 5 ms to nothing: in a batch, every worker's after-run `spec conform` paid the
  probe once per run, and `spec check` hashed the jar three times.
- One definition each of the batch option name `conform_tlc_timeout_s` (`spec.conform.CONFORM_TIMEOUT_OPTION`)
  and its default (`service.CONFORM_TLC_TIMEOUT_S`); the batch plug-in, the worker and `spec translate
  --tlc-timeout` import them.
- `service.py` and the commands import `spec.conform` at the top of the module, as `cli` already loaded it; a run
  without a bundle still never looks for java and the jar.
- The violated claims of a `spec conform` result are read from the list the result stores (`violated`) wherever
  the verdict, the batch plug-in or the console needs them; a `conform.json` from before the list was stored is
  still derived from its `properties` and `transitions`.
- `spec translate --mutate` parses each safe config once, `--reach` parses the model's symbols once per search
  rather than once per target and config, and the console names how a counterexample was found once per
  translation. Same scenarios and `translation.json`.
