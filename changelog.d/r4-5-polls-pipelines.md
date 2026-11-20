### Changed

- A failover's settle wait looks every 0.1 s instead of every 0.5 s, as the wait for a synced replica already
  did at 0.2 s. The failover action's `completed_at` ends the window in which the ledger excuses lost acknowledged
  writes of a forced failover, so that window is up to 0.4 s narrower; one forced failover on an idle 3x1 cluster
  returns in about 0.13 s instead of about 0.53 s.
- `valkey.oracle.search-results` reads an instance's hashes with one HGETALL pipeline per SCAN batch of up to 1000
  keys instead of one round trip per hash: 500 hashes take 4 round trips instead of 503, about 13 ms instead of
  about 34 ms per poll per instance. The hashes read are the same.
