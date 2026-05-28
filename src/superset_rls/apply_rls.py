"""
Apply entity-level Row-Level Security rules to Trino datasets in NAU Superset.

One "Base" RLS rule per dataset, excluding the Admin role (Base filters apply
to everyone EXCEPT the listed roles). The clause filters on the dataset's
`org_short_name` (VARCHAR) against `dim_course_access_role.org_cd` (also VARCHAR)
for the logged-in user, resolved at query time via `{{ current_username() }}`.
Group key `entity_access` so rules in the same family OR together.

Idempotent: rules are matched by the single dataset they target and updated in
place (including renaming). Rules targeting MULTIPLE datasets are flagged but
never modified. The legacy/broken `audit_rls_*` rules (which compared the
INTEGER `org_cd` column → Trino type error) are removed at the end.

ClickHouse datasets are handled by the sibling apply_rls_clickhouse.py.

Required env vars:
    SUPERSET_PASSWORD       Password for the admin user (db provider).

Optional env vars:
    SUPERSET_URL            Default: https://analitica.nau.edu.pt
    SUPERSET_USERNAME       Default: admin
    ACCESS_ROLE_TABLE       Fully-qualified access role table reachable by the
                            Superset Trino engine. Default:
                            gold_prod.audit.dim_course_access_role
    DRY_RUN                 'true' to print the plan without applying.
"""
import os
import sys
import logging
from typing import Optional

import requests

SUPERSET_URL      = os.environ.get("SUPERSET_URL", "https://analitica.nau.edu.pt").rstrip("/")
USERNAME          = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD          = os.environ.get("SUPERSET_PASSWORD")
ACCESS_ROLE_TABLE = os.environ.get("ACCESS_ROLE_TABLE", "gold_prod.audit.dim_course_access_role")
DRY_RUN           = os.environ.get("DRY_RUN", "false").lower() == "true"

GROUP_KEY        = "entity_access"
RULE_NAME_PREFIX = "rls_entity_filter__"
# Legacy bulk rules created by a previous version of this script — they compared
# the dataset's INTEGER org_cd column against the VARCHAR access-table org_cd,
# which Trino rejects (type mismatch). Removed once the per-dataset rules apply.
LEGACY_RULE_PREFIX = "audit_rls"


# =============================================================================
# CLAUSE TEMPLATES
# `{{ current_username() }}` must reach Superset un-rendered; these are plain
# strings (no f-string / .format) so the braces survive. The access role table
# is injected via the __ART__ placeholder.
# =============================================================================

_ART = "__ART__"

CLAUSE_FULL_GRAIN = """\
org_short_name IN (
    SELECT dcar.org_cd
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = true
)
OR
(org_short_name, course_cd, edition) IN (
    SELECT dcar.org_cd, dcar.course_cd, dcar.edition
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = false
      AND dcar.course_cd IS NOT NULL
      AND dcar.edition IS NOT NULL
)"""

# For datasets with org_cd + course_cd but no edition column
CLAUSE_NO_EDITION = """\
org_short_name IN (
    SELECT dcar.org_cd
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = true
)
OR
(org_short_name, course_cd) IN (
    SELECT DISTINCT dcar.org_cd, dcar.course_cd
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = false
      AND dcar.course_cd IS NOT NULL
)"""

# For "Edições e Reedições" which uses `display_number` instead of `course_cd`
CLAUSE_USING_DISPLAY_NUMBER = """\
org_short_name IN (
    SELECT dcar.org_cd
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = true
)
OR
(org_short_name, display_number, edition) IN (
    SELECT dcar.org_cd, dcar.course_cd, dcar.edition
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
      AND dcar.is_org_wide = false
      AND dcar.course_cd IS NOT NULL
      AND dcar.edition IS NOT NULL
)"""

# For datasets exposing only org granularity
# Note: no `is_org_wide = true` filter — a user sees an org if they have ANY access there
CLAUSE_ORG_ONLY = """\
org_short_name IN (
    SELECT DISTINCT dcar.org_cd
    FROM __ART__ dcar
    WHERE dcar.user_username = '{{ current_username() }}'
)"""


def _clause(template: str) -> str:
    return template.replace(_ART, ACCESS_ROLE_TABLE)


# =============================================================================
# DATASET CATALOGUE (Trino)
# (dataset_name, clause_template)
# Datasets without entity columns (Downtimes, Jira_Tickets, Utilizadores OpenEdx*)
# are intentionally absent.
# =============================================================================

DATASETS = [
    # ---- Full grain (org_short_name, course_cd, edition present)
    ("Cursos/Edições",                          CLAUSE_FULL_GRAIN),
    ("Cursos",                                  CLAUSE_FULL_GRAIN),
    ("Certificados por Inscritos (para média)", CLAUSE_FULL_GRAIN),
    ("Certificates",                            CLAUSE_FULL_GRAIN),
    ("Enrollment Flow",                         CLAUSE_FULL_GRAIN),
    ("Formandos Inscritos",                     CLAUSE_FULL_GRAIN),
    ("Inscrições vs Certificados",              CLAUSE_FULL_GRAIN),
    ("Student_performance",                     CLAUSE_FULL_GRAIN),
    ("Taxa Conclusão Actual",                   CLAUSE_FULL_GRAIN),
    ("Taxa Conclusão (desagregada)",            CLAUSE_FULL_GRAIN),
    ("Taxa Conclusão (agg)",                    CLAUSE_FULL_GRAIN),
    ("Tickets vs Cursos",                       CLAUSE_FULL_GRAIN),

    # ---- Course-level (no edition column)
    ("Cursos (estado consolidado)",             CLAUSE_NO_EDITION),

    # ---- Edition-level using display_number as course code
    ("Edições e Reedições",                     CLAUSE_USING_DISPLAY_NUMBER),

    # ---- Org-level only (no course_cd column; slightly over-permissive
    #      for course-specific users — see doc section 7)
    ("Total Formandos (ativos no período)",     CLAUSE_ORG_ONLY),
    ("Total de Formandos",                      CLAUSE_ORG_ONLY),
    ("Evolução de Entidades Ativas",            CLAUSE_ORG_ONLY),
    ("Organizações/Entidades",                  CLAUSE_ORG_ONLY),
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
        import json
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
        import json
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

    def delete_rls(self, rule_id):
        r = self.session.delete(
            f"{self.base_url}/api/v1/rowlevelsecurity/{rule_id}", timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"Delete failed ({r.status_code}): {r.text}")
        return r.json()


# =============================================================================
# HELPERS
# =============================================================================

def slugify(name: str) -> str:
    out = name.lower()
    for ch_from, ch_to in [
        ("/", "_"), (" ", "_"), ("(", ""), (")", ""),
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

    banner = f"TARGET: {SUPERSET_URL}  [Trino RLS]" + ("  [DRY RUN]" if DRY_RUN else "")
    print("\n" + "=" * (len(banner) + 4))
    print(f"= {banner} =")
    print("=" * (len(banner) + 4))
    print(f"Access role table: {ACCESS_ROLE_TABLE}\n")

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

    summary = {"create": [], "update": [], "skip": [], "missing": [],
               "deleted": [], "duplicates_warning": []}

    for ds_name, template in DATASETS:
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
            "clause": _clause(template).strip(),
            "tables": [ds_id],
            "roles": [admin_id],
            "description": f"Entity-based RLS for {ds_name}. Auto-generated by apply_rls.py.",
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

    # ---- Cleanup: remove legacy/broken audit_rls_* rules ----
    log.info("--- Cleanup: legacy '%s*' rules", LEGACY_RULE_PREFIX)
    for rule in existing.values():
        if (rule.get("name") or "").startswith(LEGACY_RULE_PREFIX):
            log.info("  [DELETE] legacy rule '%s' (id=%d)", rule["name"], rule["id"])
            if not DRY_RUN:
                api.delete_rls(rule["id"])
            summary["deleted"].append(rule["name"])

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
    print(f"  Deleted:    {len(summary['deleted'])}")
    for n in summary["deleted"]:
        print(f"    x {n}")
    print(f"  Missing:    {len(summary['missing'])}")
    for n in summary["missing"]:
        print(f"    [WARN] {n}")
    if summary["duplicates_warning"]:
        print(f"  Warnings:   {len(summary['duplicates_warning'])}")
        for w in summary["duplicates_warning"]:
            print(f"    [WARN] {w}")


if __name__ == "__main__":
    main()
