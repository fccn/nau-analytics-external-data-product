"""
Apply Row-Level Security rules in Superset based on dim_course_access_role.

Idempotent: rules are looked up by name and updated in place; re-running does
not duplicate.

Required env vars:
    SUPERSET_PASSWORD       Password for the admin user (db provider).

Optional env vars:
    SUPERSET_URL            Default: https://analitica.nau.edu.pt
    SUPERSET_USERNAME       Default: admin
    ACCESS_ROLE_TABLE       Fully-qualified access role table reachable by the
                            Superset SQL engine. Default: gold_prod.audit.dim_course_access_role
    ALLOWED_DATABASES       Comma-separated list of Superset *database* names
                            whose datasets are eligible for RLS. Datasets in
                            other databases are skipped — the EXISTS subquery
                            references the access role table by name and only
                            engines that can resolve it should be filtered.
                            Default: Trino
    DRY_RUN                 'true' to print the plan without applying.

What it does:
    1. Lists every dataset in the allowed databases.
    2. Inspects each dataset's columns and bins it into one of:
         A. has org_cd + course_cd + edition  → fine-grained rule
         B. has org_cd only                   → org-wide rule
         C. anything else                     → skipped (printed at the end)
    3. Upserts two RLS rules (one per group), scoped to all roles except Admin.

Rule semantics:
    Group A clause:
        EXISTS (SELECT 1 FROM <ACCESS_ROLE_TABLE> r
                WHERE r.user_username = '{{ current_username() }}'
                  AND r.org_cd = org_cd
                  AND (r.is_org_wide
                       OR (r.course_cd = course_cd AND r.edition = edition)))

    Group B clause:
        EXISTS (SELECT 1 FROM <ACCESS_ROLE_TABLE> r
                WHERE r.user_username = '{{ current_username() }}'
                  AND r.org_cd = org_cd)

    A user with no row in dim_course_access_role sees no data — by design.
"""
import os
import sys
import requests

SUPERSET_URL       = os.environ.get("SUPERSET_URL", "https://analitica.nau.edu.pt").rstrip("/")
USERNAME           = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD           = os.environ.get("SUPERSET_PASSWORD")
ACCESS_ROLE_TABLE  = os.environ.get("ACCESS_ROLE_TABLE", "gold_prod.audit.dim_course_access_role")
ALLOWED_DATABASES  = {d.strip() for d in os.environ.get("ALLOWED_DATABASES", "Trino").split(",") if d.strip()}
DRY_RUN            = os.environ.get("DRY_RUN", "false").lower() == "true"

RULE_A_NAME  = "audit_rls_org_course_edition"
RULE_B_NAME  = "audit_rls_org_only"
GROUP_KEY    = "audit_rls"

# {{ current_username() }} must reach Superset un-rendered — Python f-strings
# eat single braces, so we double them.
CLAUSE_A = (
    f"EXISTS (SELECT 1 FROM {ACCESS_ROLE_TABLE} r "
    f"WHERE r.user_username = '{{{{ current_username() }}}}' "
    f"AND r.org_cd = org_cd "
    f"AND (r.is_org_wide OR (r.course_cd = course_cd AND r.edition = edition)))"
)
CLAUSE_B = (
    f"EXISTS (SELECT 1 FROM {ACCESS_ROLE_TABLE} r "
    f"WHERE r.user_username = '{{{{ current_username() }}}}' "
    f"AND r.org_cd = org_cd)"
)


def login() -> requests.Session:
    if not PASSWORD:
        sys.exit("ABORT: set SUPERSET_PASSWORD env var")
    s = requests.Session()
    r = s.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db"},
    )
    r.raise_for_status()
    s.headers.update({
        "Authorization": f"Bearer {r.json()['access_token']}",
        "Content-Type":  "application/json",
        "Referer":       SUPERSET_URL,
    })
    csrf = s.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/").json()["result"]
    s.headers.update({"X-CSRFToken": csrf})
    return s


def paginate(s: requests.Session, path: str, page_size: int = 100) -> list:
    out, page = [], 0
    while True:
        r = s.get(f"{SUPERSET_URL}{path}", params={"q": f"(page:{page},page_size:{page_size})"})
        r.raise_for_status()
        result = r.json().get("result", [])
        if not result:
            break
        out.extend(result)
        if len(result) < page_size:
            break
        page += 1
    return out


def get_dataset_detail(s: requests.Session, dataset_id: int) -> dict:
    r = s.get(f"{SUPERSET_URL}/api/v1/dataset/{dataset_id}")
    r.raise_for_status()
    return r.json()["result"]


def upsert_rule(
    s: requests.Session,
    existing_rules: list,
    name: str,
    clause: str,
    table_ids: list,
    role_ids: list,
) -> None:
    payload = {
        "name":        name,
        "description": "Auto-managed by tools/superset_rls/apply_rls.py",
        "filter_type": "Regular",
        "tables":      table_ids,
        "roles":       role_ids,
        "group_key":   GROUP_KEY,
        "clause":      clause,
    }
    existing = next((r for r in existing_rules if r.get("name") == name), None)
    action = "UPDATE" if existing else "CREATE"

    if not table_ids:
        print(f"[SKIP]   {action} '{name}' — no datasets matched this group")
        return

    if DRY_RUN:
        print(f"[DRY-RUN] {action} '{name}' tables={len(table_ids)} roles={len(role_ids)}")
        return

    if existing:
        r = s.put(f"{SUPERSET_URL}/api/v1/rowlevelsecurity/{existing['id']}", json=payload)
    else:
        r = s.post(f"{SUPERSET_URL}/api/v1/rowlevelsecurity/", json=payload)
    if not r.ok:
        sys.exit(f"FAILED upsert {name}: {r.status_code} {r.text}")
    print(f"[OK]      {action} '{name}': tables={len(table_ids)} roles={len(role_ids)}")


def main() -> None:
    s = login()

    print(f"Superset:           {SUPERSET_URL}")
    print(f"Allowed databases:  {sorted(ALLOWED_DATABASES)}")
    print(f"Access role table:  {ACCESS_ROLE_TABLE}")
    print(f"Dry run:            {DRY_RUN}")

    print("\nFetching roles…")
    roles = paginate(s, "/api/v1/security/roles/")
    role_ids = [r["id"] for r in roles if r["name"] != "Admin"]
    print(f"  Non-Admin roles: {[r['name'] for r in roles if r['name'] != 'Admin']}")

    print("\nFetching datasets…")
    datasets = paginate(s, "/api/v1/dataset/")
    print(f"  Total: {len(datasets)}")

    group_a, group_b, skipped = [], [], []
    for ds in datasets:
        db_name = (ds.get("database") or {}).get("database_name")
        if db_name not in ALLOWED_DATABASES:
            continue

        ds_id   = ds["id"]
        ds_name = ds.get("table_name") or f"<id={ds_id}>"
        try:
            detail = get_dataset_detail(s, ds_id)
        except requests.HTTPError as e:
            print(f"  [WARN] could not load dataset {ds_id} ({ds_name}): {e}")
            continue
        cols = {c["column_name"] for c in detail.get("columns", [])}

        if {"org_cd", "course_cd", "edition"}.issubset(cols):
            group_a.append((ds_id, ds_name))
        elif "org_cd" in cols:
            group_b.append((ds_id, ds_name))
        else:
            skipped.append((ds_id, ds_name))

    print(f"\nGroup A (org_cd + course_cd + edition): {len(group_a)}")
    for ds_id, ds_name in sorted(group_a, key=lambda x: x[1]):
        print(f"  - {ds_name} (id={ds_id})")
    print(f"\nGroup B (org_cd only): {len(group_b)}")
    for ds_id, ds_name in sorted(group_b, key=lambda x: x[1]):
        print(f"  - {ds_name} (id={ds_id})")
    print(f"\nSkipped (no org_cd in {sorted(ALLOWED_DATABASES)} datasets): {len(skipped)}")
    for ds_id, ds_name in sorted(skipped, key=lambda x: x[1]):
        print(f"  - {ds_name} (id={ds_id})")

    print("\nFetching existing RLS rules…")
    existing_rules = paginate(s, "/api/v1/rowlevelsecurity/")

    print("\nApplying rules…")
    upsert_rule(s, existing_rules, RULE_A_NAME, CLAUSE_A,
                [ds_id for ds_id, _ in group_a], role_ids)
    upsert_rule(s, existing_rules, RULE_B_NAME, CLAUSE_B,
                [ds_id for ds_id, _ in group_b], role_ids)

    print("\nDone.")


if __name__ == "__main__":
    main()
