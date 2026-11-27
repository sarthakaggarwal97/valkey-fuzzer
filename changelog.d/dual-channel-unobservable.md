### Fixed

- Deployment: `dual-channel-replication-enabled` (Valkey 8.0's), which `valkey.pool.replication` draws, is now an
  unobservable config: a build from before it starts without the option instead of dying on
  `FATAL CONFIG FILE ERROR`, which skipped the whole test as unsupported on that build. The option only changes
  how a full sync is carried, never the data the replica ends with, so every oracle stays valid without it.
