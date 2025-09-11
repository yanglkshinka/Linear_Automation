import os, sys, json, time, requests
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple, Set
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# ---------------- CONFIG ----------------

SALES_ORDERS = ["SO109616", "SO109614", "SO109612", "SO109611"] 
# SALES_ORDERS = ["SO109610"] 

PHASES = ["Sales", "Material Planning", "Production", "Quality Control", "Shipping"]

PHASE_LENGTHS_MONTHS: Dict[str, int] = {
    "Sales": 2,
    "Material Planning": 1,
    "Production": 2,
    "Quality Control": 1,
    "Shipping": 1,
}

SO_STAGGER_MONTHS = 2
BASE_DATE: Optional[datetime] = None

LEAD_ID = "70ef7c7b-49da-4cc8-8747-720b8394dbc6"

SOURCE_TEMPLATE_PROJECT_NAMES = {
  "Sales": "SO999999 Sales",
  "Material Planning": "SO999999 Material Planning",
  "Production": "SO999999 Production",
  "Quality Control": "SO999999 Quality",
  "Shipping": "SO999999 Shipping",
}

SO_RESOURCE_LINKS: Dict[str, str] = {
    "SO109616": "https://businesscentral.dynamics.com/78839bf2-9e68-4bb0-9a66-c8cdb4fddbd4/Staging/?company=Spike%20Electric%20Controls&bookmark=21_JAAAAACLAQAAAAJ7_1MATwAxADAAOQA2ADEANg&page=42&filter=%27Sales%20Header%27.%27Document%20Type%27%20IS%20%271%27",
    "SO109614": "https://businesscentral.dynamics.com/78839bf2-9e68-4bb0-9a66-c8cdb4fddbd4/Staging/?company=Spike%20Electric%20Controls&bookmark=21_JAAAAACLAQAAAAJ7_1MATwAxADAAOQA2ADEANA&page=42&filter=%27Sales%20Header%27.%27Document%20Type%27%20IS%20%271%27",
    "SO109612": "https://businesscentral.dynamics.com/78839bf2-9e68-4bb0-9a66-c8cdb4fddbd4/Staging/?company=Spike%20Electric%20Controls&bookmark=21_JAAAAACLAQAAAAJ7_1MATwAxADAAOQA2ADEAMg&page=42&filter=%27Sales%20Header%27.%27Document%20Type%27%20IS%20%271%27",
    "SO109611": "https://businesscentral.dynamics.com/78839bf2-9e68-4bb0-9a66-c8cdb4fddbd4/Staging/?company=Spike%20Electric%20Controls&bookmark=21_JAAAAACLAQAAAAJ7_1MATwAxADAAOQA2ADEAMQ&page=42&filter=%27Sales%20Header%27.%27Document%20Type%27%20IS%20%271%27",
}
# SO_RESOURCE_LINKS: Dict[str, str] = {
#      "SO109610": "https://businesscentral.dynamics.com/78839bf2-9e68-4bb0-9a66-c8cdb4fddbd4/Staging/?company=Spike%20Electric%20Controls&bookmark=21_JAAAAACLAQAAAAJ7_1MATwAxADAAOQA2ADEAMA&page=42&filter=%27Sales%20Header%27.%27Document%20Type%27%20IS%20%271%27"
#      }

LINEAR_API_URL = "https://api.linear.app/graphql"
LINEAR_API_KEY = os.getenv("LINEAR_API_KEY")
LINEAR_TEAM_ID = os.getenv("LINEAR_TEAM_ID")

SLEEP_BETWEEN_CALLS_SEC = 0.15
DRY_RUN = False
INHERIT_RELATIONS_FROM_TEMPLATES = True

# ---------------- GraphQL helpers ----------------

def die(msg: str):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(1)

def gql(query: str, variables: dict):
    headers = {"Authorization": LINEAR_API_KEY, "Content-Type": "application/json"}
    r = requests.post(LINEAR_API_URL, headers=headers, data=json.dumps({"query": query, "variables": variables}))
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(json.dumps(data["errors"]))
    return data["data"]

def iso_date(d: datetime) -> str: return d.date().isoformat()

def mutation_exists(name: str) -> bool:
    q = """query{ __schema{ mutationType{ fields{ name } } } }"""
    try:
        data = gql(q, {})
        return any(f["name"] == name for f in data["__schema"]["mutationType"]["fields"])
    except Exception:
        return False

# ---------------- Data fetchers ----------------

def get_project_id_by_name_exact(name: str) -> Optional[str]:
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
        p = data["projects"]["pageInfo"]
        if not p["hasNextPage"]: return None
        after = p["endCursor"]

def list_issue_titles_in_project(project_id: str) -> Set[str]:
    titles: Set[str] = set()
    after = None
    while True:
        q = """
        query($id:String!, $first:Int!, $after:String){
          project(id:$id){
            issues(first:$first, after:$after){
              nodes{ title }
              pageInfo{ hasNextPage endCursor }
            }
          }
        }"""
        data = gql(q, {"id": project_id, "first": 100, "after": after})
        block = data["project"]["issues"]
        for node in block["nodes"]:
            titles.add(node["title"])
        if not block["pageInfo"]["hasNextPage"]: break
        after = block["pageInfo"]["endCursor"]
    return titles

def find_issue_id_in_project_by_title(project_id: str, title: str) -> Optional[str]:
    after = None
    while True:
        q = """
        query($id:String!, $first:Int!, $after:String){
          project(id:$id){
            issues(first:$first, after:$after){
              nodes{ id title }
              pageInfo{ hasNextPage endCursor }
            }
          }
        }"""
        data = gql(q, {"id": project_id, "first": 100, "after": after})
        block = data["project"]["issues"]
        for node in block["nodes"]:
            if node["title"] == title:
                return node["id"]
        if not block["pageInfo"]["hasNextPage"]: break
        after = block["pageInfo"]["endCursor"]
    return None

def get_issue_description(issue_id: str) -> str:
    q = """query($id:String!){ issue(id:$id){ description } }"""
    try:
        data = gql(q, {"id": issue_id})
        return data["issue"]["description"] or ""
    except Exception:
        return ""

# -------- Template issues (this was missing before!) --------
def fetch_template_issues_with_labels(project_id: str) -> List[Tuple[str, str, List[str]]]:
    """Return [(title, description, [label_names...]), ...] from a source project."""
    items: List[Tuple[str, str, List[str]]] = []
    after = None
    while True:
        q = """
        query($id:String!, $first:Int!, $after:String){
          project(id:$id){
            issues(first:$first, after:$after){
              nodes{
                title
                description
                labels{ nodes{ name } }
              }
              pageInfo{ hasNextPage endCursor }
            }
          }
        }"""
        data = gql(q, {"id": project_id, "first": 100, "after": after})
        block = data["project"]["issues"]
        for node in block["nodes"]:
            labels = [l["name"] for l in node.get("labels", {}).get("nodes", [])]
            items.append((node["title"], node.get("description") or "", labels))
        if not block["pageInfo"]["hasNextPage"]: break
        after = block["pageInfo"]["endCursor"]
    return items

# ----- Labels -----

_label_cache_name_to_id: Dict[str, str] = {}

def warm_label_cache():
    after = None
    while True:
        q = """
        query($first:Int!, $after:String){
          issueLabels(first:$first, after:$after){
            nodes{ id name }
            pageInfo{ hasNextPage endCursor }
          }
        }"""
        data = gql(q, {"first": 200, "after": after})
        block = data["issueLabels"]
        for n in block["nodes"]:
            _label_cache_name_to_id[n["name"]] = n["id"]
        if not block["pageInfo"]["hasNextPage"]: break
        after = block["pageInfo"]["endCursor"]

def get_or_create_label_id(name: str) -> Optional[str]:
    if name in _label_cache_name_to_id:
        return _label_cache_name_to_id[name]
    mutation = """
    mutation($input: IssueLabelCreateInput!){
      issueLabelCreate(input:$input){
        issueLabel{ id name }
      }
    }"""
    try:
        data = gql(mutation, {"input": {"name": name}})
        lab = data["issueLabelCreate"]["issueLabel"]
        _label_cache_name_to_id[lab["name"]] = lab["id"]
        return lab["id"]
    except Exception as e:
        print(f"       [INFO] couldn't create label '{name}': {e}")
        return None

def map_label_names_to_ids(names: List[str]) -> List[str]:
    ids = []
    for nm in names:
        lid = get_or_create_label_id(nm)
        if lid: ids.append(lid)
    return ids

# ----- Project create / issues / relations -----

def create_project_blank(name: str, description: str, start_date: str, target_date: str) -> dict:
    mutation = """
    mutation($input: ProjectCreateInput!) {
      projectCreate(input: $input) {
        success
        project { id name url }
      }
    }"""
    inp = {
        "name": name,
        "description": description,   # keep <=255
        "state": "planned",
        "priority": 0,
        "startDate": start_date,
        "targetDate": target_date,
        "leadId": LEAD_ID,
        "teamIds": [LINEAR_TEAM_ID],
    }
    data = gql(mutation, {"input": inp})
    return data["projectCreate"]["project"]

def create_issue(project_id: str, title: str, description: str, label_ids: List[str]) -> dict:
    mutation = """
    mutation($input: IssueCreateInput!) {
      issueCreate(input: $input) {
        success
        issue { id title }
      }
    }"""
    inp = {
        "title": title,
        "description": description,
        "projectId": project_id,
        "teamId": LINEAR_TEAM_ID,
        "assigneeId": LEAD_ID,
        "priority": 0,
        "labelIds": label_ids or [],
    }
    data = gql(mutation, {"input": inp})
    return data["issueCreate"]["issue"]

def update_issue_description(issue_id: str, new_description: str):
    mutation = """
    mutation($id:String!, $input: IssueUpdateInput!){
      issueUpdate(id:$id, input:$input){ success }
    }"""
    gql(mutation, {"id": issue_id, "input": {"description": new_description}})

def create_dependency_relation(predecessor_project_id: str, successor_project_id: str):
    mutation = """
    mutation($input: ProjectRelationCreateInput!) {
      projectRelationCreate(input: $input) {
        projectRelation{ id type anchorType relatedAnchorType }
      }
    }"""
    inp = {
        "projectId": predecessor_project_id,
        "relatedProjectId": successor_project_id,
        "type": "dependency",        # REQUIRED by your schema
        "anchorType": "end",
        "relatedAnchorType": "start",
    }
    try:
        gql(mutation, {"input": inp})
        print("       Linked (dependency end→start)")
    except Exception as e:
        print(f"       [INFO] Could not create dependency relation (ok to ignore): {e}")

# -------- Template relations via Project.relations / inverseRelations --------
def try_fetch_template_blocks_edges() -> List[Tuple[str, str]]:
    """
    Read edges from template projects.
    Accepts type 'dependency' (new) and 'blocks/blockedBy' (old).
    Returns (phase_from, phase_to) as predecessor->successor.
    """
    name_to_phase = {v: k for k, v in SOURCE_TEMPLATE_PROJECT_NAMES.items()}
    edges: List[Tuple[str, str]] = []

    # Prefer 'relations', then also read 'inverseRelations' to be safe
    def scan(field_name: str, pid: str):
        after = None
        while True:
            q = f"""
            query($id:String!, $first:Int!, $after:String){{
              project(id:$id){{
                {field_name}(first:$first, after:$after){{
                  nodes{{
                    type
                    project {{ name }}
                    relatedProject {{ name }}
                  }}
                  pageInfo{{ hasNextPage endCursor }}
                }}
              }}
            }}"""
            data = gql(q, {"id": pid, "first": 100, "after": after})
            pr = data["project"][field_name]
            for n in pr["nodes"]:
                t = n["type"]
                a = n["project"]["name"]
                b = n["relatedProject"]["name"]
                if a in name_to_phase and b in name_to_phase:
                    if t == "dependency" or t == "blocks":
                        edges.append((name_to_phase[a], name_to_phase[b]))
                    elif t == "blockedBy":
                        edges.append((name_to_phase[b], name_to_phase[a]))
            if not pr["pageInfo"]["hasNextPage"]: break
            after = pr["pageInfo"]["endCursor"]

    for phase, proj_name in SOURCE_TEMPLATE_PROJECT_NAMES.items():
        pid = get_project_id_by_name_exact(proj_name)
        if not pid: continue
        # Try relations; if it errors, catch and skip
        try: scan("relations", pid)
        except Exception: pass
        try: scan("inverseRelations", pid)
        except Exception: pass

    # Dedup, preserve order
    seen, uniq = set(), []
    for e in edges:
        if e not in seen:
            uniq.append(e); seen.add(e)
    return uniq

# ----- Resources: create a Project Document (preferred), else Issue fallback -----

def ensure_resources_issue(project_id: str, title: str = "Resources") -> str:
    issue_id = find_issue_id_in_project_by_title(project_id, title)
    if issue_id: return issue_id
    issue = create_issue(project_id, title, "Project links and references:\n", [])
    return issue["id"]

def upsert_link_in_issue(issue_id: str, label: str, url: str):
    current = get_issue_description(issue_id)
    line = f"- [{label}]({url})"
    if line in current: return
    new_desc = (current + "\n" if current else "") + line
    try:
        update_issue_description(issue_id, new_desc)
    except Exception as e:
        print(f"       [INFO] Could not update Resources issue description: {e}")

def try_attachment_on_issue(issue_id: str, label: str, url: str) -> bool:
    if not mutation_exists("attachmentCreate"): return False
    mut = """
    mutation($input: AttachmentCreateInput!){
      attachmentCreate(input:$input){
        success
        attachment{ id url title }
      }
    }"""
    try:
        gql(mut, {"input": {"issueId": issue_id, "url": url, "title": label}})
        return True
    except Exception as e:
        print(f"       [INFO] attachmentCreate failed (ok to ignore): {e}")
        return False

# def add_project_resources_link(project_id: str, url: str, label: str = "Dynamics link"):
#     """
#     1) Try to create a Project Document (shows in Resources)
#     2) Fallback: Resources issue + attachment or markdown
#     """
#     # 1) Try documentCreate with common field names (don’t rely on introspection)
#     if mutation_exists("documentCreate"):
#         for body_field in ("content", "body", "text"):
#             try:
#                 mut = f"""
#                 mutation($input: DocumentCreateInput!) {{
#                   documentCreate(input:$input) {{ success }}
#                 }}"""
#                 inp = {"projectId": project_id, "title": label, body_field: f"{label}: {url}"}
#                 gql(mut, {"input": inp})
#                 print("       Resources: added as project document")
#                 return
#             except Exception as e:
#                 # try next body field
#                 last_doc_err = e
#         print(f"       [INFO] documentCreate failed (ok to fallback): {last_doc_err}")

#     # 2) Fallback to a Resources issue
#     issue_id = ensure_resources_issue(project_id)
#     attached = try_attachment_on_issue(issue_id, label, url)
#     if not attached:
#         upsert_link_in_issue(issue_id, label, url)
#     print("       Resources: saved in project 'Resources' issue")
def add_project_resources_link(project_id: str, url: str, label: str = "Dynamics link"):
    """
    Adds a Project → Resources link using entityExternalLinkCreate.
    - Introspects EntityExternalLinkCreateInput to discover the correct id field
    - Ensures required fields (label, url) are included
    - Falls back through common id field names if introspection is unavailable
    """
    # 1) Try to introspect the input type to see the exact fields your tenant supports
    id_field = None
    label_field = "label"
    url_field = "url"

    try:
        introspect = """
        query {
          __type(name: "EntityExternalLinkCreateInput") {
            inputFields { name type { kind name ofType { kind name } } }
          }
        }"""
        data = gql(introspect, {})
        fields = {f["name"] for f in (data.get("__type", {}) or {}).get("inputFields", [])}

        # Pick the correct id field
        for candidate in ("projectId", "id", "targetId", "entityId"):
            if candidate in fields:
                id_field = candidate
                break

        # Some tenants call the label 'title'; detect that too
        if "label" not in fields and "title" in fields:
            label_field = "title"
        # url is usually 'url'; if not, bail to fallback
        if "url" not in fields and "link" in fields:
            url_field = "link"
    except Exception:
        # Introspection might be disabled; we'll use smart fallbacks below
        pass

    # 2) Build candidate payloads (always include a label per your error)
    candidates = []
    if id_field:
        candidates.append({id_field: project_id, url_field: url, label_field: label})
    # Fallback attempts if introspection didn't tell us
    candidates += [
        {"projectId": project_id, url_field: url, label_field: label},
        {"id":        project_id, url_field: url, label_field: label},
        {"targetId":  project_id, url_field: url, label_field: label},
        {"entityId":  project_id, url_field: url, label_field: label},
    ]

    # 3) Try the mutation with each candidate until one works
    mutation = """
    mutation EntityExternalLinkCreate($input: EntityExternalLinkCreateInput!) {
      entityExternalLinkCreate(input: $input) {
        success
        entityExternalLink { id url label }
      }
    }"""

    last_err = None
    tried_signatures = []
    for payload in candidates:
        # Normalize keys (e.g., if url_field or label_field switched)
        if "url" not in payload and url_field != "url":
            payload["url"] = payload.pop(url_field)
        if "label" not in payload and label_field != "label":
            payload["label"] = payload.pop(label_field)

        tried_signatures.append(sorted(payload.keys()))
        try:
            gql(mutation, {"input": payload})
            print(f"       Resources: added via entityExternalLinkCreate with fields {sorted(payload.keys())}")
            return
        except Exception as e:
            last_err = e
            continue

    print(f"       [INFO] entityExternalLinkCreate failed. Tried field sets: {tried_signatures}. Last error: {last_err}")

# ---------------- MAIN ----------------

def main():
    if not LINEAR_API_KEY: die("LINEAR_API_KEY not set")
    if not LINEAR_TEAM_ID: die("LINEAR_TEAM_ID not set")

    try: warm_label_cache()
    except Exception as e: print(f"[WARN] Could not warm label cache: {e}")

    # Pre-calc cumulative offsets
    cumulative_offsets: Dict[str, int] = {}
    acc = 0
    for ph in PHASES:
        cumulative_offsets[ph] = acc
        acc += PHASE_LENGTHS_MONTHS[ph]

    base = BASE_DATE or datetime.utcnow().replace(tzinfo=timezone.utc)
    print(f"[INFO] Base date (SO #1): {iso_date(base)}")
    print(f"[INFO] SO stagger: +{SO_STAGGER_MONTHS} months")
    print(f"[INFO] Phase durations: {PHASE_LENGTHS_MONTHS}")
    print(f"[INFO] Lead UUID: {LEAD_ID}")

    # Template issues cache
    template_issue_cache: Dict[str, List[Tuple[str, str, List[str]]]] = {}
    for ph, src_name in SOURCE_TEMPLATE_PROJECT_NAMES.items():
        pid = get_project_id_by_name_exact(src_name)
        if not pid:
            print(f"[WARN] Source template project not found for '{ph}': {src_name}")
            template_issue_cache[ph] = []
            continue
        try:
            template_issue_cache[ph] = fetch_template_issues_with_labels(pid)
            print(f"[INFO] Loaded {len(template_issue_cache[ph])} template issues from '{src_name}'")
        except Exception as e:
            print(f"[WARN] Could not fetch template issues for '{src_name}': {e}")
            template_issue_cache[ph] = []

    # Template relations (relations / inverseRelations); fallback to default chain
    edges: List[Tuple[str, str]] = []
    if INHERIT_RELATIONS_FROM_TEMPLATES:
        try:
            edges = try_fetch_template_blocks_edges()
        except Exception as e:
            print(f"[INFO] Could not fetch template relations; falling back: {e}")

    if not edges:
        edges = [(PHASES[i], PHASES[i+1]) for i in range(len(PHASES)-1)]
        print(f"[INFO] Using default relations chain: {edges}")
    else:
        print(f"[INFO] Inheriting template relations: {edges}")

    for so_idx, so in enumerate(SALES_ORDERS):
        so_base = base + relativedelta(months=SO_STAGGER_MONTHS * so_idx)
        print(f"\n[SO] {so}  Base={iso_date(so_base)}")

        # Create/get projects
        project_ids_by_phase: Dict[str, str] = {}
        for ph in PHASES:
            start_dt = so_base + relativedelta(months=cumulative_offsets[ph])
            target_dt = start_dt + relativedelta(months=PHASE_LENGTHS_MONTHS[ph])
            start_date, target_date = iso_date(start_dt), iso_date(target_dt)

            name = f"{so} {ph}"
            desc = f"{so} – {ph}"  # <=255

            pid = get_project_id_by_name_exact(name)
            if pid:
                print(f"  [SKIP] Exists: {name}")
                project_ids_by_phase[ph] = pid
            else:
                print(f"  [NEW] {name}  Start={start_date}  Target={target_date}")
                if not DRY_RUN:
                    try:
                        proj = create_project_blank(name, desc, start_date, target_date)
                        project_ids_by_phase[ph] = proj["id"]
                        print(f"       Created → {proj['url']}")
                        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
                    except Exception as e:
                        print(f"       [ERROR] Create failed: {e}")

        # Clone issues from templates
        for ph in PHASES:
            pid = project_ids_by_phase.get(ph)
            if not pid: continue
            tmpl_issues = template_issue_cache.get(ph, [])
            if not tmpl_issues:
                print(f"       [INFO] No template issues for {ph}; skipping.")
                continue
            try:
                existing_titles = list_issue_titles_in_project(pid)
            except Exception as e:
                print(f"       [WARN] Could not list issues for {so} {ph}: {e}")
                existing_titles = set()

            for title, desc, label_names in tmpl_issues:
                if title in existing_titles:
                    print(f"       [SKIP] Issue exists: {title}")
                    continue
                label_ids = map_label_names_to_ids(label_names)
                try:
                    create_issue(pid, title, desc, label_ids)
                    print(f"       Issue created: {title}  (labels: {', '.join(label_names) if label_names else '—'})")
                    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
                except Exception as e:
                    print(f"       [ERROR] Issue '{title}' failed: {e}")

        # Add dependency edges
        for a, b in edges:
            a_id, b_id = project_ids_by_phase.get(a), project_ids_by_phase.get(b)
            if a_id and b_id and not DRY_RUN:
                create_dependency_relation(a_id, b_id)

        # Add link to Resources (doc) or fallback issue
        bc_url = SO_RESOURCE_LINKS.get(so)
        if bc_url:
            for ph in PHASES:
                pid = project_ids_by_phase.get(ph)
                if not pid: continue
                if not DRY_RUN:
                    add_project_resources_link(pid, bc_url, label="Dynamics link")
                    # add_project_resources_link(pid, bc_url)

    print("\n[DONE]")
    return 0

if __name__ == "__main__":
    if not LINEAR_API_KEY or not LINEAR_TEAM_ID:
        die("Missing env vars. Set LINEAR_API_KEY and LINEAR_TEAM_ID.")
    sys.exit(main())
