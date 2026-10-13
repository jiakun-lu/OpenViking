import pytest
from pydantic import ValidationError

from openviking.scheduling.expiry.config import ExpiryConfig
from openviking_cli.utils.config.open_viking_config import OpenVikingConfig


def test_timer_disabled_by_default_without_redis_dependency():
    assert ExpiryConfig().enabled is False


def test_enabled_expiry_requires_service_name_and_endpoint():
    with pytest.raises(ValidationError):
        ExpiryConfig(reporting_enabled=True)


def test_shard_count_is_fixed_for_the_initial_protocol():
    with pytest.raises(ValidationError):
        ExpiryConfig(shard_count=256)


@pytest.mark.parametrize("value", [0, 4097])
def test_cleanup_batch_size_is_bounded(value):
    with pytest.raises(ValidationError):
        ExpiryConfig(cleanup_batch_size=value)


def test_callback_requires_reporting_for_cooldown_reschedule():
    with pytest.raises(ValidationError):
        ExpiryConfig(
            service_name="documents",
            callback_enabled=True,
            redis={"endpoints": ["localhost:6379"]},
        )


def test_cluster_rejects_mixed_transport_schemes():
    with pytest.raises(ValidationError, match="one transport scheme"):
        ExpiryConfig(
            service_name="documents",
            reporting_enabled=True,
            redis={
                "mode": "cluster",
                "endpoints": ["redis://one:6379", "rediss://two:6379"],
            },
        )


def test_redis_password_can_be_configured_directly():
    config = ExpiryConfig(
        service_name="documents",
        reporting_enabled=True,
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
            reporting_enabled=True,
            redis={
                "endpoints": ["localhost:6379"],
                "password": "secret",
                "password_env": "REDIS_PASSWORD",
            },
        )


def test_redis_idle_backend_requires_reporting():
    with pytest.raises(ValueError, match="reporting_enabled"):
        OpenVikingConfig.from_dict(
            {
                "memory": {
                    "session_auto_commit": {
                        "enabled": True,
                        "idle_backend": "redis",
                    }
                }
            }
        )


def test_expiry_top_level_config_is_parsed():
    config = OpenVikingConfig.from_dict(
        {
            "expiry": {
                "service_name": "documents",
                "reporting_enabled": True,
                "redis": {"endpoints": ["localhost:6379"]},
            }
        }
    )

    assert config.expiry.service_name == "documents"
    assert config.expiry.reporting_enabled is True
