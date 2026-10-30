"""Builders and fakes that the framework tests share."""

from valkey_fuzzer.contracts import (
    ComponentRegistration,
    ComponentRole,
    Declarations,
    DeploymentProviderIdentity,
)

FAKE_DEPLOYMENT_PROVIDER = "tests.deployment.fake"


def component_registration(component_id="example.action.fake", role=None, *, factory=object, **options):
    """A registration of ``component_id`` in the role its ID names, declaring nothing unless ``declares`` says."""

    role = role or ComponentRole(component_id.split(".")[1])
    options.setdefault("declares", Declarations())
    return ComponentRegistration(component_id=component_id, role=role, factory=factory, **options)


def deployment_identity() -> DeploymentProviderIdentity:
    """The identity of the provider :func:`deployment_registrations` registers."""

    return DeploymentProviderIdentity(FAKE_DEPLOYMENT_PROVIDER)
