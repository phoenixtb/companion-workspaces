"""Part 27: Larkspur's event backbone. Topics as code, a strict registry, Avro producers, the lake sink, Flink.

python -m scripts.part27_backbone reset      # (make up-backbone) drop the sink, topics, registry and tables
python -m scripts.part27_backbone topics     # create the POS, clickstream and control topics, then describe them
python -m scripts.part27_backbone registry   # global rules: valid Avro, FULL_TRANSITIVE compatibility
./gradlew -q :kafka-backbone:run --args="..." # the Java producers (see the post)
python -m scripts.part27_backbone versions   # what the registry holds, via its own API and the Confluent one
python -m scripts.part27_backbone sink       # deploy the Iceberg sink and wait for both tables to catch up
python -m scripts.part27_backbone lake       # the landed tables against the topics, via PyIceberg
python -m scripts.part27_backbone flink      # (make up-flink) Flink SQL reads the POS topic via avro-confluent
"""

import argparse
import gzip
import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from scripts import part04_star as star

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--profile", "core", "--profile", "stream"]
REGISTRY = "http://localhost:8085/apis"
CONNECT = "http://localhost:8083"
SINK = ROOT / "kafka/connectors/iceberg-sink.json"
SINK_NAME = "larkspur-iceberg-sink"
POS_TOPIC = "larkspur.pos.basket-events"
CLICK_TOPIC = "larkspur.clickstream.events"
CONTROL_TOPIC = "control-iceberg"
TABLES = {POS_TOPIC: "stream.pos_basket_events", CLICK_TOPIC: "stream.click_events"}
DAY_MS = 86_400_000
POS_DAY = "2026-08-15"  # what the producers replay (see the post)
CLICK_DAYS = ("2026-08-14", "2026-08-15")
FLINK_SQL = "part27/pos_avro.sql"

# Topics as code: partitions, key and retention are decided here, before the first producer, not by auto-creation.
TOPICS = {
    # key till_id: a till's baskets stay in order, and a re-sent basket lands on the partition of its first copy
    POS_TOPIC: {"partitions": 6, "config": {"retention.ms": 7 * DAY_MS, "cleanup.policy": "delete"}},
    # key anonymous_id: one visitor's events in order, which sessionization (Part 6) needs
    CLICK_TOPIC: {"partitions": 6, "config": {"retention.ms": 3 * DAY_MS, "cleanup.policy": "delete"}},
    # the Iceberg sink's coordination topic (one partition, as its docs create it)
    CONTROL_TOPIC: {"partitions": 1, "config": {"retention.ms": 7 * DAY_MS}},
}


def kafka(*args: str, tool: str = "kafka-topics.sh", check: bool = True) -> str:
    cmd = [*COMPOSE, "exec", "-T", "kafka", f"/opt/kafka/bin/{tool}", "--bootstrap-server", "kafka:29092", *args]
    done = subprocess.run(cmd, check=check, capture_output=True, text=True)
    return done.stdout + done.stderr


def http(method: str, url: str, body: dict | None = None, ctype: str = "application/json") -> tuple[int, object]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def catalog():
    from pyiceberg.catalog import load_catalog

    polaris = "http://localhost:8181/api/catalog"
    return load_catalog(
        "lab",
        type="rest",
        uri=polaris,
        warehouse="larkspur",
        credential="root:s3cr3t",
        scope="PRINCIPAL_ROLE:ALL",
        **{
            "oauth2-server-uri": f"{polaris}/v1/oauth/tokens",
            "header.X-Iceberg-Access-Delegation": "",
            "s3.endpoint": "http://localhost:8333",
            "s3.access-key-id": "larkspur",
            "s3.secret-access-key": "larkspur-lab-secret",
            "s3.region": "us-east-1",
        },
    )


def reset_sink() -> None:
    """The connector, its consumer groups and the landed tables: a sink that starts from the beginning of the topics."""
    http("DELETE", f"{CONNECT}/connectors/{SINK_NAME}")
    for _ in range(30):
        if http("GET", f"{CONNECT}/connectors/{SINK_NAME}")[0] == 404:
            break
        time.sleep(1)
    # Connect's consumer group for the sink (its source-topic offsets) and the coordinator's group. A group can only
    # be deleted once its members have left, which takes a few seconds after the connector is gone.
    for group in (f"connect-{SINK_NAME}", f"connect-{SINK_NAME}-coord"):
        for _ in range(30):
            out = kafka("--delete", "--group", group, tool="kafka-consumer-groups.sh", check=False)
            if "successful" in out or "does not exist" in out:
                break
            time.sleep(2)
    cat = catalog()
    if ("stream",) not in cat.list_namespaces():
        cat.create_namespace("stream")
    fs = star.fs()
    for ident in cat.list_tables("stream"):
        folder = cat.load_table(ident).location().removeprefix("s3://").rstrip("/")
        cat.drop_table(ident)  # Polaris forgets the table; the files stay unless deleted (Part 1)
        fs.invalidate_cache()
        if folder.startswith("larkspur-lake/warehouse/stream/") and fs.exists(folder):
            fs.rm(folder, recursive=True)


def cmd_reset(_: argparse.Namespace) -> None:
    reset_sink()
    topics = [POS_TOPIC, CLICK_TOPIC]
    for topic in topics:
        kafka("--delete", "--if-exists", "--topic", topic)
    _wait_deleted(topics)
    # Top-level artifacts first: the nested record types they reference (com.larkspur.*) can only go after them.
    _, found = http("GET", f"{REGISTRY}/registry/v3/search/artifacts?limit=100")
    ids = sorted((a["artifactId"] for a in found["artifacts"]), key=lambda i: not i.startswith("larkspur."))
    for artifact in ids:
        if artifact.startswith(("larkspur.", "com.larkspur.")):
            http("DELETE", f"{REGISTRY}/registry/v3/groups/default/artifacts/{artifact}")
    http("DELETE", f"{REGISTRY}/registry/v3/admin/rules")
    print("reset: sink, consumer groups, topics, registry artifacts and rules, stream.* tables")


def _wait_deleted(topics: list[str]) -> None:
    for _ in range(30):  # topic deletion is asynchronous
        if not set(topics) & set(kafka("--list").split()):
            return
        time.sleep(1)


def _create(topic: str) -> None:
    spec = TOPICS[topic]
    configs = [x for k, v in spec["config"].items() for x in ("--config", f"{k}={v}")]
    kafka("--create", "--if-not-exists", "--topic", topic, "--partitions", str(spec["partitions"]), *configs)


def cmd_topics(_: argparse.Namespace) -> None:
    for topic in TOPICS:
        _create(topic)
    for topic in TOPICS:
        first = kafka("--describe", "--topic", topic).splitlines()[0]
        print(" ".join(first.split("\t")[0:1] + [p for p in first.split("\t")[1:] if p.split(":")[0] != "TopicId"]))


def cmd_registry(_: argparse.Namespace) -> None:
    for rule, config in (("VALIDITY", "FULL"), ("COMPATIBILITY", "FULL_TRANSITIVE")):
        status, _ = http("POST", f"{REGISTRY}/registry/v3/admin/rules", {"ruleType": rule, "config": config})
        if status == 409:  # exists already: update it
            http("PUT", f"{REGISTRY}/registry/v3/admin/rules/{rule}", {"ruleType": rule, "config": config})
    _, rules = http("GET", f"{REGISTRY}/registry/v3/admin/rules")
    for rule in rules:
        _, r = http("GET", f"{REGISTRY}/registry/v3/admin/rules/{rule}")
        print(f"global rule {r['ruleType']}: {r['config']}")


def cmd_versions(_: argparse.Namespace) -> None:
    for topic in (POS_TOPIC, CLICK_TOPIC):
        artifact = f"{topic}-value"
        _, versions = http("GET", f"{REGISTRY}/registry/v3/groups/default/artifacts/{artifact}/versions")
        for v in versions["versions"]:
            ids = f"globalId {v['globalId']}  contentId {v['contentId']}"
            print(f"registry v3: {artifact} version {v['version']}  {ids}")
        # The Confluent-compatible view of the same thing: subject = artifact, id = contentId.
        _, latest = http("GET", f"{REGISTRY}/ccompat/v7/subjects/{artifact}/versions/latest")
        print(f"ccompat v7:  subject {latest['subject']} version {latest['version']}  id {latest['id']}")


def java(*args: str) -> str:
    """Run the Java producers (kafka-backbone, Java 25) through Gradle; what they print."""
    cmd = [str(ROOT / "gradlew"), "-q", ":kafka-backbone:run", f"--args={' '.join(args)}"]
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def raw_lines() -> dict[str, int]:
    """What the producers read from the raw zone: the yardstick for the topics and the lake."""

    def lines(pattern: str) -> int:
        return sum(sum(1 for _ in gzip.open(f)) for f in sorted(ROOT.glob(pattern)))

    return {
        TABLES[POS_TOPIC]: lines(f"data/raw/pos/events/upload_date={POS_DAY}/*.jsonl.gz"),
        TABLES[CLICK_TOPIC]: sum(lines(f"data/raw/clickstream/events/event_date={d}/*.jsonl.gz") for d in CLICK_DAYS),
    }


def lake_rows() -> dict[str, int]:
    cat = catalog()
    out = {}
    for table in TABLES.values():
        snap = cat.load_table(table).current_snapshot() if cat.table_exists(table) else None
        out[table] = int(snap.summary["total-records"]) if snap else 0
    return out


def cmd_sink(a: argparse.Namespace) -> None:
    reset_sink()
    config = json.loads(SINK.read_text())
    if a.isolation != config["consumer.override.isolation.level"]:
        config["consumer.override.isolation.level"] = a.isolation
    status, _ = http("PUT", f"{CONNECT}/connectors/{SINK_NAME}/config", config)
    assert status in (200, 201), status
    started = time.monotonic()
    expected = raw_lines()
    last, stable = None, 0
    while time.monotonic() - started < 600:  # until both tables hold every source line, or stop growing for a minute
        rows = lake_rows()
        if rows != last:
            elapsed = time.monotonic() - started
            print(f"{elapsed:5.0f} s  " + "  ".join(f"{t} {n:,}" for t, n in rows.items()))
            last, stable = rows, 0
        elif any(rows.values()):  # the first commit takes a while: count quiet polls only after it
            stable += 1
        if all(rows[t] >= expected[t] for t in TABLES.values()) and stable >= 3 or stable >= 6:
            break
        time.sleep(10)
    state = http("GET", f"{CONNECT}/connectors/{SINK_NAME}/status")[1]
    print(f"sink {state['connector']['state']}, tasks {[t['state'] for t in state['tasks']]}, isolation {a.isolation}")


def cmd_lake(_: argparse.Namespace) -> None:
    import pyarrow.compute as pc

    cat = catalog()
    expected = raw_lines()
    print(f"{'table':26} {'raw lines':>10} {'lake rows':>10} {'event_ids':>10} {'snapshots':>9} {'files':>6}")
    for table in TABLES.values():
        t = cat.load_table(table)
        data = t.scan(selected_fields=("event_id",)).to_arrow()
        files = len(t.inspect.files())
        print(
            f"{table:26} {expected[table]:>10,} {data.num_rows:>10,} {pc.count_distinct(data['event_id']).as_py():>10,}"
            f" {len(t.snapshots()):>9} {files:>6}"
        )
    clicks = cat.load_table(TABLES[CLICK_TOPIC])
    field = clicks.schema().find_field("page_path")
    print(f"\n{TABLES[CLICK_TOPIC]}: page_path is field {field.field_id}, optional={field.optional}, added by the sink")
    rows = clicks.scan(selected_fields=("event_ts", "page_path")).to_arrow()
    day = pc.strftime(rows["event_ts"], format="%Y-%m-%d")
    for d in CLICK_DAYS:
        on_day = pc.equal(day, d)
        with_path = pc.sum(pc.and_(on_day, pc.is_valid(rows["page_path"]))).as_py()
        print(f"  {d}: {pc.sum(on_day).as_py():,} events, {with_path:,} with page_path")
    pos = cat.load_table(TABLES[POS_TOPIC])
    sample = (
        pos.scan(
            selected_fields=(
                "basket_id",
                "event_ts",
                "_kafka_metadata_partition",
                "_kafka_metadata_offset",
                "_kafka_metadata_timestamp",
            ),
            limit=1,
        )
        .to_arrow()
        .to_pylist()[0]
    )
    print(f"\n{TABLES[POS_TOPIC]}, one row's lineage columns: {sample}")


def flink_sql() -> list[str]:
    """Run flink/sql/part27/pos_avro.sql with the SQL client in the JobManager; the result table's lines."""
    out = subprocess.run(
        [*COMPOSE, "exec", "-T", "flink-jobmanager", "./bin/sql-client.sh", "-f", f"/opt/flink/lab/sql/{FLINK_SQL}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    table = []
    for line in out.splitlines():
        if "+-" in line:  # the first border is printed straight after the echoed statement
            table.append(line[line.index("+-") :])
        elif line.startswith("|"):
            table.append(line)
    return table


def flink_rows() -> dict[str, tuple[int, int, int]]:
    rows = {}
    for line in flink_sql():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 4 and cells[1].isdigit():
            rows[cells[0]] = tuple(int(c) for c in cells[1:])
    return rows


def cmd_flink(_: argparse.Namespace) -> None:
    print("\n".join(flink_sql()))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    commands = {
        "reset": cmd_reset,
        "topics": cmd_topics,
        "registry": cmd_registry,
        "versions": cmd_versions,
        "sink": cmd_sink,
        "lake": cmd_lake,
        "flink": cmd_flink,
    }
    for name in commands:
        cmd = sub.add_parser(name)
        if name == "sink":
            cmd.add_argument("--isolation", choices=("read_committed", "read_uncommitted"), default="read_committed")
    a = p.parse_args()
    commands[a.cmd](a)


if __name__ == "__main__":
    main()
