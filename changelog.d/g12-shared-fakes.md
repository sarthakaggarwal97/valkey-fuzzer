### Changed

- The test doubles stand in for Valkey's processes and cluster text once each. `tests.fakes.FakeHandle` has the
  process protocol (`pid`, `is_alive`, `wait_exit`, `send_signal`, `restart`, `started_at`, `data_dir`): a
  SIGKILL or SIGTERM marks the instance dead in `alive` until `restart`, and the signals and restarts are
  recorded; the handle doubles of the component, fault, syscall, clock-skew, election, trace and resource-pressure
  tests keep only what differs. `tests.deployment_support.cluster_nodes_line` and `cluster_nodes_text` are the one
  CLUSTER NODES template (flags, primary, epoch, link, slot tokens, bus port, ping and pong) behind every fake
  view. `deployment_support.FakeDeployment` is the one fake cluster behind `valkey.Valkey`: healthy as built from
  its `nodes` table (what `FakeCluster` was), broken by a test's `dead`, `refusing`, `down`, `wrong`, `offset`
  and `epoch_seen_by`, and `FakeDeployment.shards(mode)` is the deployment the state-check tests run against.
