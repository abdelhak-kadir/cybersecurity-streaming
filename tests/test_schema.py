"""
Schema validation tests — no live Cassandra required.
These verify that cassandra-init.cql contains the correct DDL properties
so structural regressions (missing TTL, wrong column type, etc.) are caught
before any container is started.
"""
import pathlib
import re

CQL_PATH = pathlib.Path(__file__).parent.parent / "docker" / "cassandra-init.cql"


def load_cql() -> str:
    return CQL_PATH.read_text()


def test_realtime_threats_has_24h_ttl():
    cql = load_cql()
    assert "default_time_to_live = 86400" in cql, (
        "realtime_threats must have a 24-hour TTL (86400s) so stale data expires automatically"
    )


def test_realtime_threats_ttl_on_correct_table():
    """TTL must be associated with the realtime_threats CREATE TABLE, not another table."""
    cql = load_cql()
    # Find the CREATE TABLE block for realtime_threats specifically
    create_pos = cql.find("CREATE TABLE IF NOT EXISTS realtime_threats")
    assert create_pos != -1
    block_after_create = cql[create_pos:]
    # TTL must appear before the next CREATE TABLE statement
    next_create = block_after_create.find("CREATE TABLE", 1)
    block = block_after_create if next_create == -1 else block_after_create[:next_create]
    assert "default_time_to_live = 86400" in block


def test_attack_types_is_set_not_list():
    cql = load_cql()
    # Must use SET<TEXT> so additions are idempotent
    assert "SET<TEXT>" in cql, "attack_types must be SET<TEXT> to allow atomic, idempotent additions"
    assert "LIST<TEXT>" not in cql, "LIST<TEXT> must not appear — it requires a read-modify-write which causes races"


def test_ip_threat_summary_has_no_threat_score():
    """threat_score is derived from realtime_threats at query time, not stored in the summary."""
    cql = load_cql()
    summary_block_match = re.search(
        r"CREATE TABLE IF NOT EXISTS ip_threat_summary\s*\((.+?)\);",
        cql,
        re.DOTALL,
    )
    assert summary_block_match, "ip_threat_summary table not found"
    summary_block = summary_block_match.group(1)
    assert "threat_score" not in summary_block, (
        "threat_score must not be in ip_threat_summary — storing it causes score regressions "
        "when low-severity events arrive after high-severity ones"
    )


def test_ip_threat_summary_has_no_total_alerts():
    """total_alerts is derived via COUNT(*) on realtime_threats, not stored as a counter."""
    cql = load_cql()
    summary_block_match = re.search(
        r"CREATE TABLE IF NOT EXISTS ip_threat_summary\s*\((.+?)\);",
        cql,
        re.DOTALL,
    )
    assert summary_block_match
    summary_block = summary_block_match.group(1)
    assert "total_alerts" not in summary_block


def test_no_counter_table():
    """ip_alert_counters must not be created; counters inflate permanently on batch retries."""
    cql = load_cql()
    # DROP TABLE IF EXISTS ip_alert_counters is expected (cleanup of old schema),
    # but CREATE TABLE for it must not exist.
    assert "CREATE TABLE" not in cql or "CREATE TABLE IF NOT EXISTS ip_alert_counters" not in cql, (
        "Counter table must not be created — Spark foreachBatch is at-least-once, "
        "so counter increments overcount on retried batches"
    )
    assert "CREATE TABLE IF NOT EXISTS ip_alert_counters" not in cql


def test_no_destructive_drops_in_init():
    """cassandra-init.cql must not contain DROP TABLE — drops destroy live data on every restart."""
    cql = load_cql()
    assert "DROP TABLE" not in cql, (
        "DROP TABLE must not appear in cassandra-init.cql — it destroys live data on container restart. "
        "Use docker/cassandra-reset.cql (via 'make reset-schema') for intentional schema resets only."
    )
