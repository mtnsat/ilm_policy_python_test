#!/usr/bin/env python3
# cleanup_alias_rollover.py
# pip install requests
import requests

ES = "http://192.168.40.23:9200"
AUTH = None
ALIAS = "bench-rollover"
PREFIX = "bench-rollover-"

def req(m,p,**kw):
    r = requests.request(m, ES+p, auth=AUTH, timeout=120, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"{m} {p} -> {r.status_code}\n{r.text[:800]}")
    return r

def main():
    try:
        # remove alias from all indices (if exists)
        a = requests.get(f"{ES}/_alias/{ALIAS}", auth=AUTH, timeout=30)
        if a.status_code == 200:
            actions = []
            for idx in a.json().keys():
                actions.append({"remove":{"index":idx,"alias":ALIAS}})
            if actions:
                req("POST","/_aliases", json={"actions": actions})
            print(f"Removed alias '{ALIAS}' from indices.")
    except Exception as e:
        print("Alias remove note:", e)

    # delete indices with prefix
    cat = req("GET","/_cat/indices?h=index&s=index").text.strip().splitlines()
    targets = [i for i in cat if i.startswith(PREFIX)]
    if targets:
        j = ",".join(targets)
        req("DELETE", f"/{j}")
        print(f"Deleted indices: {targets}")
    else:
        print("No indices to delete.")

if __name__ == "__main__":
    main()

