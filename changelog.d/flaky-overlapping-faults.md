### Fixed

- Tests: `test_overlapping_faults_on_one_instance_are_serialized` judged the chaos engine's conflict keys with two
  real-time backlog changes 0.05 s apart, and the known-bad half (no conflict keys) failed on CI's Python 3.12 job
  whenever the second change started after the first's undo; the changes now overlap by 0.4 s on each side. No
  assertion changes.
