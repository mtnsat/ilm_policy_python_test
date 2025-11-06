#!/usr/bin/env python3
# ag_es_centralized_ilm_alias_rollover_v4.py
# Usage: python3 ag_es_centralized_ilm_alias_rollover_v4.py
# Requires: pip install requests

import base64, gzip, json, os, time
from io import BytesIO
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

# ------------------- CONFIG -------------------
ES = "http://192.168.40.23:9200"
AUTH = None  # e.g., ("elastic", "your-password")

# naming
ALIAS = "bench-rollover"
INDEX_PREFIX = "bench-rollover-"
FIRST_INDEX = f"{INDEX_PREFIX}000001"
ILM = "bench-quick"
INDEX_TEMPLATE = "it-bench-rollover"  # for bench-rollover-*

# shard/replicas & rollover
PRIMARY_SHARDS = 3
REPLICAS = 1
MAX_PRIMARY_SHARD_SIZE = "50gb"

# quick ILM ages for demo (hot->warm->cold->delete in minutes)
WARM_AGE = "2m"
COLD_AGE = "4m"
DELETE_AGE = "77m"

# fast rollover trigger for demo (keep size trigger too)
ROLLOVER_MAX_DOCS = 200  # set small so you see multiple rollovers quickly

# ingest knobs (we’ll adapt docs/batch to server limits)
RAW_PAYLOAD_BYTES = 2 * 1024 * 1024  # 2 MiB raw => ~2.66–2.80 MiB base64/doc
CLIENT_TIMEOUT: Tuple[float, float] = (10.0, 600.0)  # (connect, read) sec
ILM_POLL_SECS = 5
TARGET_ROLLOVERS = 5                 # how many rollovers to witness
MAX_MINUTES = 60                     # safety cap for the demo
SAFETY_MARGIN = 0.85                 # stay under 85% of http.max_content_length
CAP_DOCS_PER_BULK = 40               # extra cap so gzip blocks aren't huge
# ---------------------------------------------

# Persistent session with retries/keep-alive (version-compatible)
S = requests.Session()
common_retry_args = dict(
    total=6, connect=3, read=3, backoff_factor=0.5,
    status_forcelist=(429, 502, 503, 504),
    raise_on_status=False,
)
try:
    retries = Retry(**common_retry_args, allowed_methods={"GET","POST","PUT","HEAD"})
except TypeError:
    retries = Retry(**common_retry_args, method_whitelist={"GET","POST","PUT","HEAD"})
S.mount("http://", HTTPAdapter(max_retries=retries))
S.mount("https://", HTTPAdapter(max_retries=retries))

# ------------------- HTTP helpers -------------------
def req(method: str, path: str, **kw) -> requests.Response:
    r = S.request(method, ES + path, auth=AUTH, timeout=CLIENT_TIMEOUT, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {path} -> {r.status_code}\n{r.text[:800]}")
    return r

def exists_index(index: str) -> bool:
    return S.head(f"{ES}/{index}", auth=AUTH, timeout=CLIENT_TIMEOUT).status_code == 200

def exists_alias(alias: str) -> bool:
    return S.head(f"{ES}/_alias/{alias}", auth=AUTH, timeout=CLIENT_TIMEOUT).status_code == 200

# ------------------- Step 1: ILM -------------------
def put_ilm_quick():
    body = {
        "policy": {
            "phases": {
                "hot": {
                    "actions": {
                        "set_priority": {"priority": 100},
                        "rollover": {
                            "max_primary_shard_size": MAX_PRIMARY_SHARD_SIZE,
                            "max_docs": ROLLOVER_MAX_DOCS
                        }
                    }
                },
                "warm": {"min_age": WARM_AGE,
                         "actions": {"set_priority": {"priority": 50},
                                     "forcemerge": {"max_num_segments": 1}}},
                "cold": {"min_age": COLD_AGE,
                         "actions": {"set_priority": {"priority": 0}}},
                "delete": {"min_age": DELETE_AGE,
                           "actions": {"delete": {}}}
            }
        }
    }
    req("PUT", f"/_ilm/policy/{ILM}", json=body)

# ---- Step 2a: Template so future rolled indices inherit ILM & 3P/1R ----
def ensure_rollover_template():
    body = {
        "index_patterns": [f"{INDEX_PREFIX}*"],
        "priority": 500,
        "template": {
            "settings": {
                "index.number_of_shards": PRIMARY_SHARDS,
                "index.number_of_replicas": REPLICAS,
                "index.lifecycle.name": ILM,
                "index.lifecycle.rollover_alias": ALIAS,
                "index.refresh_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "@timestamp": {"type":"date"},
                    "service.name":{"type":"keyword"},
                    "log.level":{"type":"keyword"},
                    "message":{"type":"binary"}
                },
                "dynamic_templates":[
                    {"strings_as_keywords":{
                        "match_mapping_type":"string",
                        "mapping":{"type":"keyword","ignore_above":256}
                    }}
                ]
            }
        }
    }
    req("PUT", f"/_index_template/{INDEX_TEMPLATE}", json=body)

# ---- Step 2b: Create first write index + alias (3P/1R) ----
def create_write_index_and_alias():
    if exists_index(FIRST_INDEX):
        print(f"ℹ️  Index {FIRST_INDEX} already exists; skipping create.")
        return
    if exists_alias(ALIAS):
        info = req("GET", f"/_alias/{ALIAS}").json()
        for idx, meta in info.items():
            if meta.get("aliases", {}).get(ALIAS, {}).get("is_write_index"):
                raise RuntimeError(
                    f"Alias '{ALIAS}' already has write index '{idx}'. "
                    f"Remove/rename it or change ALIAS/INDEX_PREFIX."
                )
    body = {
        "settings": {
            "index.number_of_shards": PRIMARY_SHARDS,
            "index.number_of_replicas": REPLICAS,
            "index.lifecycle.name": ILM,
            "index.lifecycle.rollover_alias": ALIAS,
            "index.refresh_interval": "30s",
        },
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "service.name": {"type": "keyword"},
                "log.level": {"type": "keyword"},
                "message": {"type": "binary"}  # big field; not indexed
            },
            "dynamic_templates":[
                {"strings_as_keywords":{
                    "match_mapping_type":"string",
                    "mapping":{"type":"keyword","ignore_above":256}
                }}
            ]
        },
        "aliases": {ALIAS: {"is_write_index": True}}
    }
    req("PUT", f"/{FIRST_INDEX}", json=body)

def get_write_index(alias: str) -> str:
    data = req("GET", f"/_alias/{alias}").json()
    for idx, meta in data.items():
        if meta.get("aliases", {}).get(alias, {}).get("is_write_index"):
            return idx
    raise RuntimeError(f"Write index for alias '{alias}' not found")

def set_fast(index: str, on: bool = True):
    settings = {"index": {"refresh_interval": "-1", "translog.durability": "async"}} if on else \
               {"index": {"refresh_interval": "30s", "translog.durability": "request"}}
    req("PUT", f"/{index}/_settings", json=settings)

# -------- server limits & batch sizing ----------
def get_http_max_content_length_bytes() -> int:
    js = req("GET", "/_cluster/settings?include_defaults=true").json()
    # Try persistent / transient first
    for root in ("persistent", "transient"):
        v = js.get(root, {}).get("http", {}).get("max_content_length")
        if isinstance(v, str):
            if v.endswith("mb"): return int(v[:-2]) * 1024 * 1024
            if v.endswith("b"):  return int(v[:-1])
    # Fallback to defaults
    v = js.get("defaults", {}).get("http", {}).get("max_content_length", "100mb")
    if isinstance(v, str):
        if v.endswith("mb"): return int(v[:-2]) * 1024 * 1024
        if v.endswith("b"):  return int(v[:-1])
    return 100 * 1024 * 1024

def docs_per_batch(avg_doc_bytes: int, max_bytes: int) -> int:
    per_doc = avg_doc_bytes + 64
    budget = int(max_bytes * SAFETY_MARGIN)
    return max(1, budget // per_doc)

# -------- bulk helpers (gzip + adapt) ----------
def bulk_ndjson(payload: str, n: int) -> str:
    lines: List[str] = []
    for _ in range(n):
        lines.append('{"index":{}}')
        lines.append(json.dumps({
            "@timestamp": "2025-11-05T12:00:00Z",
            "service.name": "bench",
            "log.level": "info",
            "message": payload
        }, separators=(",", ":")))
    return "\n".join(lines) + "\n"

def gzip_bytes(s: str) -> bytes:
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(s.encode("utf-8"))
    return buf.getvalue()

def post_bulk_with_adapt(alias: str, ndjson: str, docs_in_batch: int) -> int:
    headers = {"Content-Type": "application/x-ndjson", "Content-Encoding": "gzip"}
    payload = gzip_bytes(ndjson)
    try:
        r = S.post(f"{ES}/{alias}/_bulk", headers=headers, data=payload,
                   auth=AUTH, timeout=CLIENT_TIMEOUT)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return -1  # signal caller to shrink
    if r.status_code >= 300:
        if r.status_code in (413, 429, 502, 503, 504):
            return -1
        raise RuntimeError(f"bulk -> {r.status_code}\n{r.text[:800]}")
    js = r.json()
    if js.get("errors"):
        print("⚠️  bulk item errors present (continuing)")
    return docs_in_batch

# ---- ILM helpers ----
def ilm_explain_indices(names: List[str]) -> Dict:
    if not names: return {}
    resp = req("GET", f"/{','.join(names)}/_ilm/explain").json()
    return resp.get("indices", resp)

def ensure_index_is_managed(index: str):
    # Check if ILM is attached; if not, attach lifecycle + rollover_alias
    resp = req("GET", f"/{index}/_settings?include_defaults=true").json()
    idx_settings = next(iter(resp.values()))["settings"]["index"]
    lifecycle = idx_settings.get("lifecycle", {})
    name = (lifecycle.get("name") or
            idx_settings.get("lifecycle.name") or "")
    rollover_alias = (lifecycle.get("rollover_alias") or
                      idx_settings.get("lifecycle.rollover_alias") or "")
    if not name or not rollover_alias:
        print(f"❌ {index} missing ILM settings. Attaching lifecycle...")
        req("PUT", f"/{index}/_settings", json={
            "index.lifecycle.name": ILM,
            "index.lifecycle.rollover_alias": ALIAS
        })
        # brief pause for the coordinator to pick it up
        time.sleep(1.0)

# -------------- Step 3: ingest ----------------
def run_ingest_until_rollovers():
    payload = base64.b64encode(os.urandom(RAW_PAYLOAD_BYTES)).decode("ascii")
    avg_doc_bytes = len(payload)  # ~2.7–2.8 MB base64
    max_content = get_http_max_content_length_bytes()
    dpb = min(docs_per_batch(avg_doc_bytes, max_content), CAP_DOCS_PER_BULK)

    print(f"Server http.max_content_length ≈ {max_content/1024/1024:.0f} MB")
    print(f"Avg doc (b64): {avg_doc_bytes/1024/1024:.2f} MiB")
    print(f"Docs/bulk initial: {dpb}")

    write_idx = get_write_index(ALIAS)
    ensure_index_is_managed(write_idx)
    set_fast(write_idx, True)

    rollovers = 0
    last_poll = 0.0
    batches = 0
    start_ts = time.time()

    while True:
        ndj = bulk_ndjson(payload, dpb)
        sent = post_bulk_with_adapt(ALIAS, ndj, dpb)
        if sent < 0:
            dpb = max(1, dpb // 2)
            print(f"↘️  Shrinking docs/bulk to {dpb} (timeout/backpressure)")
            time.sleep(0.2)
            continue

        batches += 1
        time.sleep(0.02)  # short breath

        now = time.time()
        if now - last_poll >= ILM_POLL_SECS:
            current = get_write_index(ALIAS)
            if current != write_idx:
                rollovers += 1
                print(f"🎉 Rollover #{rollovers}: {write_idx} -> {current}")
                # restore old, prep new
                try: set_fast(write_idx, False)
                except Exception as e: print("note:", e)
                ensure_index_is_managed(current)  # <-- critical: auto-attach if missing
                try: set_fast(current, True)
                except Exception as e: print("note:", e)
                write_idx = current
                if rollovers >= TARGET_ROLLOVERS:
                    print("✅ Target rollovers reached. Stopping ingest.")
                    break
            last_poll = now

        if (now - start_ts) / 60 > MAX_MINUTES:
            print("⏱️  Time cap reached. Stopping ingest.")
            break

        if batches % 10 == 0:
            approx_pre_gzip = dpb * (avg_doc_bytes + 64) / (1024*1024)
            print(f"...{batches} bulks, docs/bulk={dpb}, ~{approx_pre_gzip:.1f} MB (pre-gzip) each")

    # restore defaults on current write
    try: set_fast(write_idx, False)
    except Exception as e: print("note:", e)

# -------------- Step 4: report ----------------
def cat_alias() -> str:
    return req("GET", f"/_cat/aliases/{ALIAS}?v").text

def stats_for_indices(indices: List[str]) -> Dict:
    return req("GET", "/"+",".join(indices)+"/_stats/store,docs").json()

def list_indices_with_prefix(prefix: str) -> List[str]:
    txt = req("GET", "/_cat/indices?h=index&s=index").text.strip()
    return [line for line in txt.splitlines() if line.startswith(prefix)]

def print_report():
    print("\n================= FINAL REPORT =================")
    try:
        print("\n[Alias mapping]")
        print(cat_alias().strip())
    except Exception as e:
        print("alias note:", e)

    idxs = list_indices_with_prefix(INDEX_PREFIX)
    if idxs:
        print("\n[Index list]")
        for i in idxs: print(" -", i)
        try:
            st = stats_for_indices(idxs).get("indices", {})
            print("\n[Sizes (primaries vs total)]")
            for name in idxs:
                info = st.get(name, {})
                p = info.get("primaries", {}).get("store", {}).get("size_in_bytes", 0)
                t = info.get("total", {}).get("store", {}).get("size_in_bytes", 0)
                print(f" {name:>28}  primaries={p/1024/1024/1024:.2f} GiB  total={t/1024/1024/1024:.2f} GiB")
        except Exception as e:
            print("stats note:", e)
        try:
            exp = ilm_explain_indices(idxs)
            print("\n[ILM state (phase/action/step)]")
            if not exp:
                print("  (No indices to explain yet.)")
            else:
                for name in idxs:
                    s = exp.get(name, {})
                    phase = s.get("phase"); action = s.get("action"); step = s.get("step")
                    print(f" {name:>28}  phase={phase}  action={action}  step={step}")
        except Exception as e:
            print("ilm explain note:", e)
    else:
        print("\n[Index list] none found with prefix", INDEX_PREFIX)
    print("================================================\n")

# ------------------- main -------------------
def main():
    print("🛠️  Creating quick ILM policy...")
    put_ilm_quick()

    print("🧩 Ensuring index template for rollovers (3P/1R + ILM)...")
    ensure_rollover_template()

    print("📦 Creating initial write index + alias (3P/1R)...")
    create_write_index_and_alias()

    print("🚀 Ingesting until rollovers occur (adaptive, gzipped bulks)...")
    run_ingest_until_rollovers()

    print_report()

if __name__ == "__main__":
    main()

