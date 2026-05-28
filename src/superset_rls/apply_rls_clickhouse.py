"""
Apply entity-level Row-Level Security rules to ClickHouse datasets in NAU Superset.

Sibling of apply_rls.py (which handles Trino datasets).

ClickHouse cannot join the Trino permissions table, so the clause is a single
Jinja macro call — `{{ user_accessible_courses_ck(...) }}`. The macro (registered
via JINJA_CONTEXT_ADDONS in the Superset config, managed in the k8s manifests
repo) looks up the current user's permissions in Trino's dim_course_access_role
and emits an inline IN-list clause. One "Base" rule per dataset, excluding Admin.
Group key `entity_access` (same family as Trino — they OR together).

Idempotent: rules are matched by the single dataset they target and updated in
place (including renaming, e.g. a leftover rls_test_* rule). Rules targeting
MULTIPLE datasets are flagged but never modified.

Prerequisite: the macro user_accessible_courses_ck must be registered in the
Superset config (already deployed via the kubernetes-manifests repo).

Required env vars:
    SUPERSET_PASSWORD       Password for the admin user (db provider).

Optional env vars:
    SUPERSET_URL            Default: https://analitica.nau.edu.pt
    SUPERSET_USERNAME       Default: admin
    DRY_RUN                 'true' to print the plan without applying.
"""
import os
import sys
import json
import logging
from typing import Optional

import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "https://analitica.nau.edu.pt").rstrip("/")
USERNAME     = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD     = os.environ.get("SUPERSET_PASSWORD")
DRY_RUN      = os.environ.get("DRY_RUN", "false").lower() == "true"

GROUP_KEY        = "entity_access"
RULE_NAME_PREFIX = "rls_entity_filter__"


# =============================================================================
# DATASET CATALOGUE — 13 ClickHouse datasets
# (dataset_name, clause). All clauses are one-line Jinja macro calls; the macro
# itself lives in the Superset config and does the Trino lookup + caching.
# =============================================================================

DATASETS = [
    # ---- Full grain — datasets that expose course_cd + edition
    ("Dropout",                                    "{{ user_accessible_courses_ck() }}"),
    ("Atividades Clickhouse",                      "{{ user_accessible_courses_ck() }}"),
    ("% utilizadores com visualização dos vídeos", "{{ user_accessible_courses_ck() }}"),

    # ---- Full grain — datasets that expose `curso` instead of course_cd
    ("Frequência Média Login",            '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Interação nos Fóruns",              '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Média último Dia Entrada no Curso", '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Número Sessões para Conclusão",     '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Quizzes",                           '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Sessões Clickhouse",                '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("Sessões Clickhouse (Concluídos)",   '{{ user_accessible_courses_ck(course_col="curso") }}'),
    ("% de interações por fórum",         '{{ user_accessible_courses_ck(course_col="curso") }}'),

    # ---- Org-only fallback — datasets without course/edition columns
    ("Nota Média Global",           '{{ user_accessible_courses_ck(mode="org_only") }}'),
    ("Nota Média Global por Bloco", '{{ user_accessible_courses_ck(mode="org_only") }}'),
]


# =============================================================================
# SUPERSET API CLIENT
# =============================================================================

class SupersetAPI:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.username = username
        self.password = password

    def login(self):
        r = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={"username": self.username, "password": self.password,
                  "provider": "db", "refresh": True},
            timeout=30,
        )
        r.raise_for_status()
        self.session.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        r = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/", timeout=30)
        r.raise_for_status()
        self.session.headers["X-CSRFToken"] = r.json()["result"]
        self.session.headers["Referer"] = self.base_url

    def get_admin_role_id(self) -> int:
        r = self.session.get(
            f"{self.base_url}/api/v1/security/roles/",
            params={"q": '(filters:!((col:name,opr:eq,value:Admin)))'},
            timeout=30,
        )
        r.raise_for_status()
        results = r.json().get("result", [])
        if not results:
            raise RuntimeError("Admin role not found")
        return results[0]["id"]

    def get_dataset_id(self, name: str) -> Optional[int]:
        r = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            params={"q": json.dumps({
                "filters": [{"col": "table_name", "opr": "eq", "value": name}]
            })},
            timeout=30,
        )
        r.raise_for_status()
        results = [x for x in r.json().get("result", []) if x.get("table_name") == name]
        return results[0]["id"] if results else None

    def list_rls_rules(self) -> dict:
        rules, page = {}, 0
        while True:
            r = self.session.get(
                f"{self.base_url}/api/v1/rowlevelsecurity/",
                params={"q": json.dumps({"page": page, "page_size": 100})},
                timeout=30,
            )
            r.raise_for_status()
            batch = r.json().get("result", [])
            for rule in batch:
                rules[rule["id"]] = rule
            if len(batch) < 100:
                break
            page += 1
        return rules

    def create_rls(self, payload):
        r = self.session.post(
            f"{self.base_url}/api/v1/rowlevelsecurity/", json=payload, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Create failed ({r.status_code}): {r.text}")
        return r.json()

    def update_rls(self, rule_id, payload):
        r = self.session.put(
            f"{self.base_url}/api/v1/rowlevelsecurity/{rule_id}", json=payload, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Update failed ({r.status_code}): {r.text}")
        return r.json()


# =============================================================================
# HELPERS
# =============================================================================

def slugify(name: str) -> str:
    out = name.lower()
    for ch_from, ch_to in [
        ("/", "_"), (" ", "_"), ("(", ""), (")", ""), ("%", "pct"),
        ("ç", "c"), ("ã", "a"), ("õ", "o"),
        ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
        ("ê", "e"), ("ô", "o"), ("â", "a"),
    ]:
        out = out.replace(ch_from, ch_to)
    return out


def _normalize_clause(s: Optional[str]) -> str:
    return " ".join((s or "").split())


def diff_rule(existing: dict, desired: dict) -> list:
    diffs = []
    if (existing.get("name") or "") != desired["name"]:
        diffs.append("name")
    if _normalize_clause(existing.get("clause")) != _normalize_clause(desired["clause"]):
        diffs.append("clause")
    if existing.get("filter_type") != desired["filter_type"]:
        diffs.append("filter_type")
    if (existing.get("group_key") or "") != desired["group_key"]:
        diffs.append("group_key")
    if sorted(t["id"] for t in existing.get("tables", [])) != sorted(desired["tables"]):
        diffs.append("tables")
    if sorted(r["id"] for r in existing.get("roles", [])) != sorted(desired["roles"]):
        diffs.append("roles")
    return diffs


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    # Pod/CI stdout may be a non-UTF-8 locale; accented dataset names would
    # otherwise raise UnicodeEncodeError. Force UTF-8 with safe fallback.
    for _stream in (sys.stdout, sys.stderr):
        _reconfig = getattr(_stream, "reconfigure", None)
        if _reconfig:
            try:
                _reconfig(encoding="utf-8", errors="replace")
            except Exception:
                pass

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger(__name__)

    if not PASSWORD:
        sys.exit("ABORT: set SUPERSET_PASSWORD env var")

    banner = f"TARGET: {SUPERSET_URL}  [ClickHouse RLS]" + ("  [DRY RUN]" if DRY_RUN else "")
    print("\n" + "=" * (len(banner) + 4))
    print(f"= {banner} =")
    print("=" * (len(banner) + 4) + "\n")

    api = SupersetAPI(SUPERSET_URL, USERNAME, PASSWORD)
    log.info("Logging in to %s ...", SUPERSET_URL)
    api.login()

    admin_id = api.get_admin_role_id()
    log.info("Admin role id = %d", admin_id)

    log.info("Fetching existing RLS rules ...")
    existing = api.list_rls_rules()
    log.info("Found %d existing rules", len(existing))

    rules_by_dataset = {}
    for rule in existing.values():
        for t in rule.get("tables", []):
            rules_by_dataset.setdefault(t["id"], []).append(rule)

    summary = {"create": [], "update": [], "skip": [], "missing": [], "duplicates_warning": []}

    for ds_name, clause in DATASETS:
        target_rule_name = RULE_NAME_PREFIX + slugify(ds_name)
        log.info("--- Dataset: %s", ds_name)

        ds_id = api.get_dataset_id(ds_name)
        if ds_id is None:
            log.warning("  [WARN] Dataset not found in Superset")
            summary["missing"].append(ds_name)
            continue

        existing_for_ds = rules_by_dataset.get(ds_id, [])
        single_target = [r for r in existing_for_ds if len(r.get("tables", [])) == 1]
        multi_target = [r for r in existing_for_ds if len(r.get("tables", [])) > 1]
        for r in multi_target:
            warn = f"{ds_name} -> rule '{r['name']}' targets {len(r['tables'])} datasets - not modifying"
            log.warning("  [WARN] %s", warn)
            summary["duplicates_warning"].append(warn)

        desired = {
            "name": target_rule_name,
            "filter_type": "Base",
            "group_key": GROUP_KEY,
            "clause": clause.strip(),
            "tables": [ds_id],
            "roles": [admin_id],
            "description": f"Entity-based RLS for ClickHouse dataset {ds_name}. Auto-generated.",
        }

        if not single_target:
            log.info("  + CREATE %s", target_rule_name)
            if not DRY_RUN:
                api.create_rls(desired)
            summary["create"].append(target_rule_name)
        else:
            if len(single_target) > 1:
                names = [r["name"] for r in single_target]
                warn = f"{ds_name} has {len(single_target)} single-target rules: {names}. Updating first."
                log.warning("  [WARN] %s", warn)
                summary["duplicates_warning"].append(warn)
            single_target.sort(key=lambda r: r["id"])
            current = single_target[0]
            diffs = diff_rule(current, desired)
            if diffs:
                log.info("  [UPDATE] rule '%s' (id=%d), diffs: %s",
                         current["name"], current["id"], diffs)
                if not DRY_RUN:
                    api.update_rls(current["id"], desired)
                summary["update"].append((current["name"], current["id"], diffs))
            else:
                log.info("  [OK] Rule '%s' (id=%d) already up-to-date", current["name"], current["id"])
                summary["skip"].append(current["name"])

    # ---- Summary ----
    print()
    print("=" * 70)
    print("SUMMARY" + (" (DRY RUN - no changes applied)" if DRY_RUN else ""))
    print("=" * 70)
    print(f"  Created:    {len(summary['create'])}")
    for n in summary["create"]:
        print(f"    + {n}")
    print(f"  Updated:    {len(summary['update'])}")
    for n, _id, diffs in summary["update"]:
        print(f"    ~ {n} (id={_id})  {','.join(diffs)}")
    print(f"  Unchanged:  {len(summary['skip'])}")
    print(f"  Missing:    {len(summary['missing'])}")
    for n in summary["missing"]:
        print(f"    [WARN] {n}")
    if summary["duplicates_warning"]:
        print(f"  Warnings:   {len(summary['duplicates_warning'])}")
        for w in summary["duplicates_warning"]:
            print(f"    [WARN] {w}")


if __name__ == "__main__":
    main()
