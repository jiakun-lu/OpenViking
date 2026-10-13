import pytest

from openviking.scheduling.expiry import (
    ExpiryError,
    ExpiryMode,
    ExpiryRegistration,
    ItemIdentity,
)
from openviking.scheduling.expiry.keys import (
    ExpiryKeys,
    decode_component,
    decode_item_id,
    encode_component,
    encode_item_id,
    shard_for,
)
from openviking.scheduling.expiry.models import (
    SCORE_TIME_BASE,
    BucketRef,
    OwnerRole,
    bucket_minute_ms,
    claim_layer_bounds,
    decode_claim_score,
    encode_claim_score,
    encode_ready_claim_score,
    metadata_ttl_ms,
    validate_report,
)


def test_identity_round_trip_is_canonical_and_isolated():
    identity = ItemIdentity("session", "account:a", "用户/{x}", "session:1")
    item_id = encode_item_id(identity)
    assert decode_item_id(item_id) == identity
    assert "用户" not in item_id
    assert "{" not in item_id
    assert shard_for(item_id, 512) == shard_for(item_id, 512)


@pytest.mark.parametrize("encoded", ["=", "YQ==", "***", "_"])
def test_decode_component_rejects_non_canonical_values(encoded):
    with pytest.raises(ExpiryError, match="encoded identity"):
        decode_component(encoded)


def test_component_round_trip_preserves_unicode():
    value = "账户/with spaces:✓"
    assert decode_component(encode_component(value)) == value


def test_timer_keys_only_use_a_control_hash_tag_where_required():
    keys = ExpiryKeys(encode_component("cluster/resource"))
    assert keys.service_key == encode_component("cluster/resource")
    ref = BucketRef("session", 7, 120_000)
    assert "{" not in keys.source(7)
    assert "{" not in keys.bucket(ref)
    tag = keys.control_tag(7)
    assert keys.progress(7) == f"resource_sche:process:consume_progress:{tag}"
    assert keys.registry(7) == f"resource_consume:bucket_register:{tag}"
    scheduler_owner = keys.owner(OwnerRole.SCHEDULER, 7)
    callback_owner = keys.owner(OwnerRole.CALLBACK, 7, item_type="session")
    assert scheduler_owner == f"resource_timer:owner:scheduler:{tag}"
    assert callback_owner == f"resource_timer:owner:callback:session:{tag}"


def test_callback_owner_requires_item_type_scope():
    keys = ExpiryKeys("resource")
    with pytest.raises(ValueError, match="requires an item type"):
        keys.owner(OwnerRole.CALLBACK, 7)
    with pytest.raises(ValueError, match="must not include"):
        keys.owner(OwnerRole.SCHEDULER, 7, item_type="session")


@pytest.mark.parametrize(
    ("timeout", "expected"),
    [(3600, 86_400_000), (43_200, 86_400_000), (46_800, 93_600_000)],
)
def test_metadata_ttl(timeout, expected):
    assert metadata_ttl_ms(timeout) == expected


def test_minute_and_claim_boundaries():
    assert bucket_minute_ms(60_000) == 60_000
    assert bucket_minute_ms(60_001) == 120_000
    score = encode_claim_score(2, 123)
    assert decode_claim_score(score) == (2, 123)
    assert claim_layer_bounds(0) == (0, SCORE_TIME_BASE - 1)
    assert claim_layer_bounds(2) == (
        2 * SCORE_TIME_BASE + 1,
        3 * SCORE_TIME_BASE - 1,
    )
    assert encode_ready_claim_score(2) == 2 * SCORE_TIME_BASE + 1


def test_report_rejects_fixed_mode_and_boolean_numbers():
    identity = ItemIdentity("session", "a", "u", "s")
    with pytest.raises(ExpiryError) as exc:
        validate_report(ExpiryRegistration(identity, 100, 1, ExpiryMode.FIXED))
    assert exc.value.code == "unsupported_mode"
    with pytest.raises(ExpiryError) as exc:
        validate_report(ExpiryRegistration(identity, True, 1))
    assert exc.value.code == "invalid_report"
