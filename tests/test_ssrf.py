import pytest

from app.callback import CallbackUrlError, validate_callback_url


def test_rejects_non_http_scheme():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("file:///etc/passwd", allow_local=False)
    with pytest.raises(CallbackUrlError):
        validate_callback_url("gopher://example.com/", allow_local=False)


def test_rejects_userinfo():
    with pytest.raises(CallbackUrlError):
        validate_callback_url(
            "http://user:pass@example.com/cb", allow_local=False
        )


def test_rejects_loopback_literal():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http://127.0.0.1:8000/cb", allow_local=False)


def test_rejects_link_local_metadata():
    with pytest.raises(CallbackUrlError):
        validate_callback_url(
            "http://169.254.169.254/latest/meta-data/", allow_local=False
        )


def test_rejects_private_rfc1918():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http://10.0.0.5/cb", allow_local=False)
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http://192.168.1.1/cb", allow_local=False)


def test_rejects_ipv6_loopback():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http://[::1]/cb", allow_local=False)


def test_rejects_unresolvable_host():
    with pytest.raises(CallbackUrlError):
        validate_callback_url(
            "http://this-host-does-not-exist.consuma.invalid/", allow_local=False
        )


def test_rejects_empty_or_oversize():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("", allow_local=False)
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http://x/" + ("a" * 3000), allow_local=False)


def test_rejects_missing_host():
    with pytest.raises(CallbackUrlError):
        validate_callback_url("http:///path", allow_local=False)


def test_allow_local_permits_loopback():
    validate_callback_url("http://127.0.0.1:8000/cb", allow_local=True)
    validate_callback_url("http://[::1]/cb", allow_local=True)


def test_allow_local_still_blocks_link_local_metadata():
    # The whole point of allow_local is convenient local demos --
    # not "disable SSRF protection entirely". Cloud metadata IPs
    # must remain blocked.
    with pytest.raises(CallbackUrlError):
        validate_callback_url(
            "http://169.254.169.254/latest/meta-data/", allow_local=True
        )
