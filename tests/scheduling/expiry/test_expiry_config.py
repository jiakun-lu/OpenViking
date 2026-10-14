import traceback

import pytest
from pydantic import ValidationError

from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig
from openviking_cli.utils.config.open_viking_config import OpenVikingConfig


def test_publisher_uses_separate_process_by_default():
    assert ExpiryConfig().publisher_separate_process is True


def test_incomplete_expiry_config_is_allowed_when_redis_backend_is_disabled():
    config = OpenVikingConfig.from_dict({"expiry": {"service_name": ""}})

    assert config.expiry.redis.endpoints == []


def test_expiry_capacity_defaults():
    config = ExpiryConfig()

    assert config.shard_count == 128
    assert config.session_consumer_concurrency == 8


@pytest.mark.parametrize("value", [1, 64, 128, 512])
def test_shard_count_accepts_configured_value(value):
    assert ExpiryConfig(shard_count=value).shard_count == value


@pytest.mark.parametrize("value", [0, 513])
def test_shard_count_is_bounded(value):
    with pytest.raises(ValidationError):
        ExpiryConfig(shard_count=value)


@pytest.mark.parametrize("value", [1, 8, 1024])
def test_session_consumer_concurrency_accepts_positive_value(value):
    assert ExpiryConfig(session_consumer_concurrency=value).session_consumer_concurrency == value


def test_session_consumer_concurrency_must_be_positive():
    with pytest.raises(ValidationError):
        ExpiryConfig(session_consumer_concurrency=0)


@pytest.mark.parametrize("value", [0, 4097])
def test_cleanup_batch_size_is_bounded(value):
    with pytest.raises(ValidationError):
        ExpiryConfig(cleanup_batch_size=value)


@pytest.mark.parametrize(
    "field",
    ["reporting_enabled", "scheduler_enabled", "callback_enabled"],
)
def test_legacy_role_switches_are_rejected(field):
    with pytest.raises(ValidationError, match=field):
        ExpiryConfig.model_validate({field: True})


def test_cluster_rejects_mixed_transport_schemes():
    with pytest.raises(ValidationError, match="one transport scheme"):
        ExpiryConfig(
            service_name="documents",
            redis={
                "mode": "cluster",
                "endpoints": ["redis://one:6379", "rediss://two:6379"],
            },
        )


def test_redis_password_can_be_configured_directly():
    config = ExpiryConfig(
        service_name="documents",
        redis={
            "endpoints": ["localhost:6379"],
            "password": "secret",
        },
    )

    assert config.redis.password == "secret"


def test_redis_password_sources_are_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        ExpiryConfig(
            service_name="documents",
            redis={
                "endpoints": ["localhost:6379"],
                "password": "secret",
                "password_env": "REDIS_PASSWORD",
            },
        )


@pytest.mark.parametrize("password", ["x", "private-redis-password-" * 20])
@pytest.mark.parametrize("level", ["redis", "expiry", "aggregate"])
@pytest.mark.parametrize("failure", ["endpoint", "password_sources", "field_type"])
def test_redis_config_validation_traceback_does_not_expose_password(password, level, failure):
    redis = {"endpoints": ["localhost:6379"], "password": password}
    if failure == "endpoint":
        redis["endpoints"] = ["invalid-endpoint"]
    elif failure == "password_sources":
        redis["password_env"] = "REDIS_PASSWORD"
    else:
        redis["max_connections"] = "invalid"
    with pytest.raises((ValueError, ValidationError)) as error:
        if level == "redis":
            ExpiryRedisConfig.model_validate(redis)
        elif level == "expiry":
            ExpiryConfig.model_validate({"redis": redis})
        else:
            OpenVikingConfig.from_dict({"expiry": {"redis": redis}})
    formatted = "".join(traceback.format_exception(error.value))
    # Short passwords are tested as raw input values rather than arbitrary
    # characters which can also occur in ordinary error text.
    assert f"'password': '{password}'" not in formatted
    if len(password) > 1:
        assert password not in formatted
    assert "input_value=" not in formatted


@pytest.mark.parametrize(
    "endpoint",
    [
        "redis://user:embedded-secret@localhost",
        "redis://user:embedded-secret@localhost:bad-port",
        "redis://user:embedded-secret@[bad-address]:6379",
        "redis://user:embedded-secret@localhost:6379",
    ],
)
def test_redis_endpoint_validation_never_echoes_credentials(endpoint):
    with pytest.raises(ValidationError) as error:
        ExpiryRedisConfig(endpoints=[endpoint])
    assert "embedded-secret" not in "".join(traceback.format_exception(error.value))


def test_aggregate_validation_hides_redis_password_for_unrelated_error():
    with pytest.raises(ValueError) as error:
        OpenVikingConfig.from_dict(
            {"default_account": [], "expiry": {"redis": {"password": "private-password"}}}
        )
    assert "private-password" not in "".join(traceback.format_exception(error.value))


def test_password_env_must_exist_when_resolved(monkeypatch):
    monkeypatch.delenv("MISSING_REDIS_PASSWORD", raising=False)
    config = ExpiryRedisConfig(password_env="MISSING_REDIS_PASSWORD")

    with pytest.raises(ValueError, match="MISSING_REDIS_PASSWORD"):
        config.resolve_password()


def test_password_env_is_resolved_without_exposing_it_in_endpoint(monkeypatch):
    monkeypatch.setenv("EXPIRY_REDIS_PASSWORD", "secret")
    config = ExpiryRedisConfig(password_env="EXPIRY_REDIS_PASSWORD")

    assert config.resolve_password() == "secret"


def test_redis_endpoint_rejects_embedded_credentials():
    with pytest.raises(ValidationError, match="dedicated Redis fields"):
        ExpiryRedisConfig(endpoints=["redis://user:secret@localhost:6379"])


@pytest.mark.parametrize(
    ("expiry", "message"),
    [
        ({}, "service_name"),
        ({"service_name": "documents"}, "redis.endpoints"),
    ],
)
def test_redis_idle_backend_requires_complete_expiry_config(expiry, message):
    with pytest.raises(ValueError, match=message):
        OpenVikingConfig.from_dict(
            {
                "memory": {
                    "session_auto_commit": {
                        "enabled": True,
                        "idle_backend": "redis",
                    }
                },
                "expiry": expiry,
            }
        )


def test_expiry_top_level_config_is_parsed():
    config = OpenVikingConfig.from_dict(
        {
            "expiry": {
                "service_name": "documents",
                "publisher_separate_process": False,
                "shard_count": 64,
                "session_consumer_concurrency": 16,
                "redis": {"endpoints": ["localhost:6379"]},
            },
            "memory": {
                "session_auto_commit": {
                    "enabled": True,
                    "idle_backend": "redis",
                }
            },
        }
    )

    assert config.expiry.service_name == "documents"
    assert config.expiry.publisher_separate_process is False
    assert config.expiry.shard_count == 64
    assert config.expiry.session_consumer_concurrency == 16
