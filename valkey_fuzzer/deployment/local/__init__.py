"""The built-in local deployment: Valkey instances as processes on this host.

- :mod:`.provider` registers ``valkey-fuzzer.deployment.local`` and owns the
  deployment's lifecycle; :mod:`.service` is the service components use.
- :mod:`.config` parses the deployment document, :mod:`.planner` plans the
  instances and applies startup overlays, :mod:`.ports` allocates ports,
  :mod:`.processes` runs valkey-server, and :mod:`.formation` sets up
  replication and cluster mode.
- :mod:`.failover` promotes replicas, and :mod:`.validation` checks the
  deployment's state.
- :mod:`.rerun` moves a generated scenario off busy ports and compares the
  binaries a replay would run with the recorded ones.
- :mod:`.models`, :mod:`.server_config`, :mod:`.queries`, and
  :mod:`.cluster_nodes` hold the shared models and helpers.
"""
