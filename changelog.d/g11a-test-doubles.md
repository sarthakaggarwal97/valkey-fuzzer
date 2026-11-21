### Changed

- The test doubles two or more test files shared have support homes, so no test module imports another:
  `tests/network_fakes.py` holds the `Iptables` and `Tc` fakes (the slot-migration chaos test's reduced
  `_Iptables` copy is gone; it uses the full fake), `tests/ledger_support.py` the key-value workload's
  `StoreClient` and `standalone` handle, and `tests/valkey_experiments_support.spec_for` the resolved spec of a
  component by use, id, role and config. `test_cluster_chaos.FakeCluster` is `FakeChaosCluster`, no longer a
  namesake of `deployment_support.FakeCluster`.
