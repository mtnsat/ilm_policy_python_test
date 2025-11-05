#!/usr/bin/env python3
# cleanup_datastream.py
# pip install requests
import requests

ES = "http://192.168.40.23:9200"
AUTH = None

DS = "bench-logs"
IT_NAME = "it-bench-logs"
CT_SETTINGS = "bench-logs@settings"
CT_MAPPINGS = "bench-logs@mappings"
ILM = "bench-90d"  # optional to delete

def req(m,p,**kw):
    r = requests.request(m, ES+p, auth=AUTH, timeout=120, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"{m} {p} -> {r.status_code}\n{r.text[:800]}")
    return r

def main():
    # delete data stream (deletes backing indices)
    r = requests.get(f"{ES}/_data_stream/{DS}", auth=AUTH, timeout=30)
    if r.status_code == 200:
        req("DELETE", f"/_data_stream/{DS}")
        print(f"Deleted data stream '{DS}' and backing indices.")
    else:
        print("Data stream not present.")

    # delete index template
    try:
        req("DELETE", f"/_index_template/{IT_NAME}")
        print(f"Deleted index template '{IT_NAME}'.")
    except Exception as e:
        print("Index template delete note:", e)

    # delete component templates
    for ct in [CT_SETTINGS, CT_MAPPINGS]:
        try:
            req("DELETE", f"/_component_template/{ct}")
            print(f"Deleted component template '{ct}'.")
        except Exception as e:
            print(f"Component template delete note ({ct}):", e)

    # optional: delete ILM policy (only if not shared!)
    try:
        req("DELETE", f"/_ilm/policy/{ILM}")
        print(f"Deleted ILM policy '{ILM}'.")
    except Exception as e:
        print("ILM delete note:", e)

if __name__ == "__main__":
    main()

