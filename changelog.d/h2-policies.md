### Removed: the `policies` scenario section (H2)

- The `policies` scenario section, `PolicySpec`, the plan's `policies` field, the coverage tuple's `policies` and
  `policy_mix` entries, the `policies` batch matrix root, and the `policies` parameter of
  `EnvironmentProvisioner.acquire` and `stable_environment_order`. The section was parsed, validated, planned,
  recorded and passed through, and read by nothing: no component or provider ever took a policy, and no shipped
  experiment, fragment, example or zoo case declared one. A scenario with `policies:` now fails strict parsing as
  any unknown section does. A `plan.json` or `report.json` written before this still loads: the plan loader drops
  the `"policies": []` such files recorded; replaying one reports plan-digest drift, as any code change does.
