#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, requests
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())   # reads .env in your workspace

LINEAR_API_URL = "https://api.linear.app/graphql"
LINEAR_API_KEY = os.getenv("LINEAR_API_KEY")   # raw key; no "Bearer "
SLEEP_BETWEEN_CALLS_SEC = 0.1

# ---- set to False to actually delete/archive ----
DRY_RUN = False

# Four SOs to wipe:
SO_LIST = ["SO109616", "SO109614", "SO109612", "SO109610"]
# SO_LIST = ["SO109611"]
PHASES  = ["Sales", "Material Planning", "Production", "Quality Control", "Shipping"]

# Build full list of project names to delete
PROJECT_NAMES = [f"{so} {phase}" for so in SO_LIST for phase in PHASES]

def gql(query: str, variables: dict):
    headers = {"Authorization": LINEAR_API_KEY, "Content-Type": "application/json"}
    r = requests.post(LINEAR_API_URL, headers=headers, data=json.dumps({"query": query, "variables": variables}))
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]

def get_project_id_by_name_exact(name: str):
    after = None
    while True:
        q = """
        query($first:Int!, $after:String){
          projects(first:$first, after:$after){
            nodes{ id name }
            pageInfo{ hasNextPage endCursor }
          }
        }"""
        data = gql(q, {"first": 100, "after": after})
        for n in data["projects"]["nodes"]:
            if n["name"] == name:
                return n["id"]
        page = data["projects"]["pageInfo"]
        if not page["hasNextPage"]:
            return None
        after = page["endCursor"]

def delete_project(project_id: str) -> bool:
    mutation = """
    mutation($id:String!){
      projectDelete(id:$id){ success }
    }"""
    try:
        data = gql(mutation, {"id": project_id})
        return bool(data["projectDelete"]["success"])
    except Exception:
        return False

def archive_project(project_id: str) -> bool:
    mutation = """
    mutation($id:String!){
      projectArchive(id:$id){ success }
    }"""
    try:
        data = gql(mutation, {"id": project_id})
        return bool(data["projectArchive"]["success"])
    except Exception:
        return False

def main():
    if not LINEAR_API_KEY:
        print("[FATAL] LINEAR_API_KEY not set"); return 1

    print(f"[INFO] DRY_RUN = {DRY_RUN}")
    print(f"[INFO] Will process {len(PROJECT_NAMES)} projects:")
    for n in PROJECT_NAMES: print("  -", n)

    for name in PROJECT_NAMES:
        pid = get_project_id_by_name_exact(name)
        if not pid:
            print(f"[MISS] {name}  (not found)")
            continue

        if DRY_RUN:
            print(f"[WOULD DELETE] {name}  (id={pid})")
        else:
            if delete_project(pid):
                print(f"[DELETED] {name}")
            elif archive_project(pid):
                print(f"[ARCHIVED] {name} (delete not permitted)")
            else:
                print(f"[ERROR] Could not delete or archive {name}")

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
