#!/usr/bin/env python3
# datastream_rollover_quicktest.py
# pip install requests

import base64, json, math, os, time, requests

ES = "http://192.168.40.23:9200"
AUTH = None

DS = "bench-logs"
ILM = "bench-90d"
PRIMARY_SHARDS = 3
REPLICAS = 1
MAX_PRIMARY_SHARD_SIZE = "50gb"
REFRESH = "30s"

CT_SETTINGS = "bench-logs@settings"
CT_MAPPINGS = "bench-logs@mappings"
IT_NAME = "it-bench-logs"

# speed tuning
RAW_PAYLOAD_BYTES = 2 * 1024 * 1024
HTTP_BULK_BUDGET_MB = 95
ILM_POLL_SECS = 5

def req(method, path, **kw):
    r = requests.request(method, ES + path, auth=AUTH, timeout=180, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {path} -> {r.status_code}\n{r.text[:800]}")
    return r

def put_ilm():
    body = {
        "policy": {
            "phases": {
                "hot":   {"actions": {"rollover": {"max_primary_shard_size": MAX_PRIMARY_SHARD_SIZE}}},
                "warm":  {"min_age": "2d", "actions": {"forcemerge": {"max_num_segments": 1}}},
                "cold":  {"min_age": "30d", "actions": {}},
                "delete":{"min_age": "90d", "actions": {"delete": {}}}
            }
        }
    }
    req("PUT", f"/_ilm/policy/{ILM}", json=body)

def put_components_and_template():
    req("PUT", f"/_component_template/{CT_SETTINGS}", json={
        "template": {"settings": {
            "index.number_of_shards": PRIMARY_SHARDS,
            "index.number_of_replicas": REPLICAS,
            "index.lifecycle.name": ILM,
            "index.refresh_interval": REFRESH
        }},
        "version": 1
    })
    req("PUT", f"/_component_template/{CT_MAPPINGS}", json={
        "template": {"mappings": {
            "properties": {
                "@timestamp":{"type":"date"},
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
        }},
        "version": 1
    })
    req("PUT", f"/_index_template/{IT_NAME}", json={
        "index_patterns":[f"{DS}*"],
        "data_stream": {},
        "priority": 500,
        "composed_of":[CT_SETTINGS, CT_MAPPINGS]
    })

def ensure_ds():
    r = requests.get(f"{ES}/_data_stream/{DS}", auth=AUTH, timeout=60)
    if r.status_code == 404:
        req("PUT", f"/_data_stream/{DS}")

def ds_write_index():
    info = req("GET", f"/_data_stream/{DS}").json()
    ds = info["data_streams"][0]
    return ds["indices"][-1]["index_name"]

def set_fast(index, on=True):
    if on:
        s = {"index":{"refresh_interval":"-1","translog.durability":"async"}}
    else:
        s = {"index":{"refresh_interval":REFRESH,"translog.durability":"request"}}
    req("PUT", f"/{index}/_settings", json=s)

def make_payload():
    return base64.b64encode(os.urandom(RAW_PAYLOAD_BYTES)).decode("ascii")

def docs_per_batch(avg_doc_bytes, budget_mb):
    per_doc = avg_doc_bytes + 32
    return max(1, (budget_mb*1024*1024) // per_doc)

def bulk_lines(payload, n):
    parts = []
    for _ in range(n):
        parts.append('{"create":{}}')
        parts.append(json.dumps({
            "@timestamp":"2025-11-05T12:00:00Z",
            "service.name":"bench",
            "log.level":"info",
            "message": payload
        }, separators=(",",":")))
    return "\n".join(parts) + "\n"

def post_bulk(ndjson):
    r = requests.post(f"{ES}/{DS}/_bulk",
                      headers={"Content-Type":"application/x-ndjson"},
                      data=ndjson, auth=AUTH, timeout=300)
    if r.status_code >= 300:
        raise RuntimeError(f"bulk -> {r.status_code}\n{r.text[:800]}")
    js = r.json()
    if js.get("errors"):
        raise RuntimeError("bulk item errors")

def main():
    print("Creating ILM + templates + data stream...")
    put_ilm()
    put_components_and_template()
    ensure_ds()
    write0 = ds_write_index()
    set_fast(write0, True)

    payload = make_payload()
    avg = len(payload)
    dpb = docs_per_batch(avg, HTTP_BULK_BUDGET_MB)
    print(f"Write backing index: {write0}")
    print(f"Avg doc ~{avg/1024/1024:.2f} MiB, docs/batch={dpb}")

    last_poll = 0.0
    batches = 0
    while True:
        ndj = bulk_lines(payload, dpb)
        post_bulk(ndjson=ndj)
        batches += 1
        time.sleep(0.02)

        now = time.time()
        if now - last_poll >= ILM_POLL_SECS:
            w = ds_write_index()
            if w != write0:
                print(f"🎉 Rollover: {write0} -> {w}")
                # restore both
                try: set_fast(write0, False)
                except: pass
                try: set_fast(w, False)
                except: pass
                break
            last_poll = now

        if batches % 20 == 0:
            print(f"...sent {batches} bulks (~{batches*HTTP_BULK_BUDGET_MB} MB)")

    print("Verify:")
    print("  GET _data_stream/bench-logs")
    print("  GET _ilm/explain")
    print("  GET .ds-bench-logs-*/_stats/store,docs")

if __name__ == "__main__":
    main()

