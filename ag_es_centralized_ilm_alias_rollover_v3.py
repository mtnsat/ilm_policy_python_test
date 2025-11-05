#!/usr/bin/env python3
# consolidated_alias_rollover_quicktest_es9_v3.py
# pip install requests

import base64, gzip, json, os, time
from io import BytesIO
from typing import Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

# ------------------- CONFIG -------------------
ES = "http://192.168.40.23:9200"
AUTH = None  # e.g., ("elastic", "your-password")

ALIAS = "bench-rollover"
INDEX_PREFIX = "bench-rollover-"
FIRST_INDEX = f"{INDEX_PREFIX}000001"
ILM = "bench-quick"

PRIMARY_SHARDS = 3
REPLICAS = 1
MAX_PRIMARY_SHARD_SIZE = "50gb"

WARM_AGE = "2m"
COLD_AGE = "4m"
DELETE_AGE = "7m"
ROLLOVER_MAX_DOCS = 500  # demo speed

# ingest knobs (we’ll adapt docs/batch to server limits)
RAW_PAYLOAD_BYTES = 2 * 1024 * 1024  # 2 MiB raw → ~2.66 MiB base64
CLIENT_TIMEOUT: Tuple[float, float] = (10.0, 600.0)  # (connect, read) seconds
ILM_POLL_SECS = 5
TARGET_ROLLOVERS = 3
MAX_MINUTES = 60
SAFETY_MARGIN = 0.85  # stay under 85% of http.max_content_length

# ---------------------------------------------

# Persistent session with retries/keep-alive
S = requests.Session()

# Version-compatible Retry config
common_retry_args = dict(
    total=6,
    connect=3,
    read=3,
    backoff_factor=0.5,
    status_forcelist=(429, 502, 503, 504),
    raise_on_status=False,
)
# urllib3 < 1.26 uses method_whitelist — newer uses allowed_methods
try:
    retries = Retry(**common_retry_args, allowed_methods={"GET", "POST", "PUT", "HEAD"})
except TypeError:
    retries = Retry(**common_retry_args, method_whitelist={"GET", "POST", "PUT", "HEAD"})

    
S.mount("http://", HTTPAdapter(max_retries=retries))
S.mount("https://", HTTPAdapter(max_retries=retries))

def req(method: str, path: str, **kw) -> requests.Response:
    r = S.request(method, ES + path, auth=AUTH, timeout=CLIENT_TIMEOUT, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {path} -> {r.status_code}\n{r.text[:800]}")
    return r

def exists_index(index: str) -> bool:
    return S.head(f"{ES}/{index}", auth=AUTH, timeout=CLIENT_TIMEOUT).status_code == 200

def exists_alias(alias: str) -> bool:
    return S.head(f"{ES}/_alias/{alias}", auth=AUTH, timeout=CLIENT_TIMEOUT).status_code == 200

# ---------- ILM ----------
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

# ---------- index + alias ----------
def create_write_index_and_alias():
    if exists_index(FIRST_INDEX):
        print(f"ℹ️  Index {FIRST_INDEX} already exists; skipping create.")
        return

    if exists_alias(ALIAS):
        info = req("GET", f"/_alias/{ALIAS}").json()
        for idx, meta in info.items():
            if meta.get("aliases", {}).get(ALIAS, {}).get("is_write_index"):
                raise RuntimeError(
                    f"Alias '{ALIAS}' already has write index '{idx}'. Adjust ALIAS/INDEX_PREFIX."
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
                "message": {"type": "binary"}  # big field not indexed
            },
            "dynamic_templates": [
                {"strings_as_keywords": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword", "ignore_above": 256}
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

# ---------- server limits & batch sizing ----------
def get_http_max_content_length_bytes() -> int:
    # include defaults to read the effective value
    js = req("GET", "/_cluster/settings?include_defaults=true").json()
    # default in ES is 100mb unless changed
    # navigate defaults -> http -> max_content_length
    def find(path_list, dct):
        cur = dct
        for p in path_list:
            cur = cur.get(p, {})
        return cur

    # Try explicit in persistent or transient first
    for root in ("persistent", "transient"):
        val = find(["http", "max_content_length"], js.get(root, {}))
        if isinstance(val, str) and val.endswith("mb"):
            return int(val[:-2]) * 1024 * 1024
        if isinstance(val, str) and val.endswith("b"):
            return int(val[:-1])

    # Fallback to defaults
    val = find(["http", "max_content_length"], js.get("defaults", {}))
    if isinstance(val, str) and val.endswith("mb"):
        return int(val[:-2]) * 1024 * 1024
    if isinstance(val, str) and val.endswith("b"):
        return int(val[:-1])

    # If unknown, assume conservative 100MB
    return 100 * 1024 * 1024

def docs_per_batch(avg_doc_bytes: int, max_bytes: int) -> int:
    # rough per-doc overhead for action/meta + newline
    per_doc = avg_doc_bytes + 64
    # hold back to safety margin
    budget = int(max_bytes * SAFETY_MARGIN)
    return max(1, budget // per_doc)

# ---------- bulk helpers (gzip + retries + adapt) ----------
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
    """
    Returns the docs actually sent (could be fewer if we split due to errors).
    Adapts by halving batch on any timeout/connection error.
    """
    headers = {
        "Content-Type": "application/x-ndjson",
        "Content-Encoding": "gzip"
    }
    payload = gzip_bytes(ndjson)

    try:
        r = S.post(f"{ES}/{alias}/_bulk", headers=headers, data=payload,
                   auth=AUTH, timeout=CLIENT_TIMEOUT)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        # halve the batch size signal to caller
        return -1

    if r.status_code >= 300:
        # if ES returns 413 (too large) or 429/backpressure, tell caller to shrink
        if r.status_code in (413, 429, 502, 503, 504):
            return -1
        raise RuntimeError(f"bulk -> {r.status_code}\n{r.text[:800]}")
    js = r.json()
    if js.get("errors"):
        # surface first err to logs; still count as sent but advise shrinking
        print("⚠️  bulk item errors present (consider shrinking batch)")
    return docs_in_batch

# ---------- ingest loop ----------
def run_ingest_until_rollovers():
    payload = base64.b64encode(os.urandom(RAW_PAYLOAD_BYTES)).decode("ascii")
    avg_doc_bytes = len(payload)  # ~2.8MB base64
    max_content = get_http_max_content_length_bytes()
    dpb = docs_per_batch(avg_doc_bytes, max_content)

    # cap docs/bulk further to avoid large gzip blocks if you want:
    dpb = min(dpb, 40)

    print(f"Server http.max_content_length ≈ {max_content/1024/1024:.0f} MB")
    print(f"Avg doc (b64): {avg_doc_bytes/1024/1024:.2f} MiB")
    print(f"Docs/bulk initial: {dpb}")

    write_idx = get_write_index(ALIAS)
    set_fast(write_idx, True)

    rollovers = 0
    last_poll = 0.0
    batches = 0
    start_ts = time.time()

    while True:
        ndj = bulk_ndjson(payload, dpb)
        sent = post_bulk_with_adapt(ALIAS, ndj, dpb)
        if sent < 0:
            # shrink batch and retry next loop
            dpb = max(1, dpb // 2)
            print(f"↘️  Shrinking docs/bulk to {dpb} (timeout/backpressure)")
            time.sleep(0.2)
            continue

        batches += 1
        # tiny pause
        time.sleep(0.02)

        now = time.time()
        if now - last_poll >= ILM_POLL_SECS:
            current = get_write_index(ALIAS)
            if current != write_idx:
                rollovers += 1
                print(f"🎉 Rollover #{rollovers}: {write_idx} -> {current}")
                # restore old, speed up new
                try: set_fast(write_idx, False)
                except Exception as e: print("note:", e)
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
            approx_uncompressed = dpb * (avg_doc_bytes + 64) / (1024*1024)
            print(f"...{batches} bulks, docs/bulk={dpb}, ~{approx_uncompressed:.1f} MB (pre-gzip) each")

    # restore defaults on current write
    try: set_fast(write_idx, False)
    except Exception as e: print("note:", e)

# ---------- reporting ----------
def cat_alias() -> str:
    return req("GET", f"/_cat/aliases/{ALIAS}?v").text

def stats_for_indices(indices: List[str]) -> Dict:
    return req("GET", "/"+",".join(indices)+"/_stats/store,docs").json()

def list_indices_with_prefix(prefix: str) -> List[str]:
    txt = req("GET", "/_cat/indices?h=index&s=index").text.strip()
    return [line for line in txt.splitlines() if line.startswith(prefix)]

def ilm_explain() -> Dict:
    idxs = list_indices_with_prefix(INDEX_PREFIX)
    if not idxs:
        return {}
    resp = req("GET", f"/{','.join(idxs)}/_ilm/explain").json()
    return resp["indices"] if "indices" in resp else resp

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
            exp = ilm_explain()
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

def main():
    print("🛠️  Creating quick ILM policy...")
    put_ilm_quick()

    print("📦 Creating initial write index + alias (3P/1R)...")
    create_write_index_and_alias()

    print("🚀 Ingesting until rollovers occur (adaptive, gzipped bulks)...")
    run_ingest_until_rollovers()

    print_report()
    print("Done. (Cleanup is manual as requested.)")

if __name__ == "__main__":
    main()

