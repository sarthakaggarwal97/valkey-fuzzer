### Changed: one home per documented fact (G14; P-65, P-66, P-68 to P-72)

- The docs say each of these once and link to it from everywhere else: the order a run looks for `valkey-server`
  in and the build cache's path (`docs/getting-started.md#choose-a-valkey-binary`; `docs/operations.md`,
  `docs/architecture.md` and the `### Binaries` section of `docs/scenarios-and-experiments.md` link to it), the
  server config defaults (`docs/scenarios-and-experiments.md#server-configuration`; `docs/architecture.md` links
  to it), the outcome table with each outcome's meaning (`docs/contracts.md#outcomes-and-verdicts`;
  `docs/getting-started.md` keeps a two-column outcome and exit-code table), how two builds are compared
  (`docs/scenarios-and-experiments.md#comparing-two-builds`, a new section holding what the page said after the
  experiment table; `docs/cli.md` and `docs/getting-started.md` link to it), and the `tla2tools.jar` download
  with its checksum (`docs/spec-bridge.md#java-and-the-jar`, a new section; the four `formal/*/README.md` keep
  their `spec check` command and link to it).
- `tests/test_docs_spec_claims.py` guards each bundle README's "Files" table: every `negative_controls` entry of
  `claims.yaml` has a row that names its file and says `must violate` its property, no row names a control the
  claims do not, and each control file exists. The "checked against a live run" column it generates now takes the
  `spec conform` half from each property's `conform` in `claims.yaml`; only the oracle that checks a property
  live, and how, is written by hand.

### Fixed

- `docs/architecture.md`: the provisioning step listed `cluster-node-timeout 1000` twice in one sentence, and the
  "not built" list said replay refuses on drift. Replay never refuses: it warns, records the drift in the report,
  and runs (`docs/cli.md#replay`); the getting-started troubleshooting row that named a `refusing to replay`
  error that no command prints now names the warning a replay does print.
