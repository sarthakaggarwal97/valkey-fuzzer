### Changed

- `kill_server` finds a server's children in `/proc/<pid>/task/*/children` (Linux) instead of scanning a
  `ps -A` listing of every process, so a SIGKILL lands about 26 ms sooner on a host with 800 processes
  (0.03 ms instead of 26 ms); the `ps` listing stays as the fallback where `/proc` has no `children` files.
  The same children are killed.
