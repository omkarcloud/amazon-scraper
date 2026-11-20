"""Test helpers shared by the site tests.

The one module here that looks at `config`: it has to tell the hosted service,
whose config knows about proxies and exit policies, from an open-source repo,
whose config only names an optional proxy."""
import pytest


def _hosted():
    try:
        import config
    except ImportError:
        return False
    return bool(getattr(config, "HOSTED", False))


# For a test of the hosted exit policy (which proxy a session rides, what a
# block moves it to). It runs in the service's own tree and is skipped in an
# open-source repo, where requests simply go out direct.
hosted_only = pytest.mark.skipif(not _hosted(), reason="tests the hosted service's exit policy; this repo runs direct")
