import os
from dataclasses import dataclass
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.gold_utils_functions import update_ctrl_table, get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# ============================================================
# Aggregation table definitions
# ============================================================

@dataclass
class AggTable:
    name: str
    sql_fn: Callable[..., str]     # sql_fn(tgt_layer) or sql_fn(tgt_layer, day_filter_sql)
    partition_by: str
    sort_order: str
    # FIX 3: target output partitions per table — controls file count written to S3.
    # Lower values = fewer, larger files = faster S3 writes.
    # Tune per table based on expected output size.
    output_partitions: int = 50
    # Set to True for tables without a day_key grain that require a full
    # replace on every run instead of partition-level incremental overwrite.
    full_refresh: bool = False
    # FIX 8: When True, the sql_fn accepts a second arg (SQL predicate fragment)
    # that gets injected into the fact-table scan so the incremental filter is
    # applied BEFORE the expensive joins/aggregation, not after. The outer
    # INNER JOIN _changed_days in _overwrite_changed_partitions is kept as a
    # safety belt — even if the pushed filter were buggy, dynamic overwrite
    # can still only touch partitions in _changed_days.
    pushdown_day_filter: bool = False


# ────────────────────────────────────────────────────────────
# Superset Dataset: Taxa Conclusão Final
# FIX 6: The original query was a raw UNION ALL with NO aggregation,
# producing the full row count of fact_certificate_daily +
# fact_course_enrollment_daily (potentially hundreds of millions of rows).
# Combined with repartition(10), this concentrated massive data per
# partition, causing repeated executor OOM kills and cascading shuffle
# fetch failures — the query never completed in 4+ hours.
# Pre-aggregation to (course_edition_key, org_key) grain reduces output
# from hundreds of millions of rows to a few thousand.
#
# FIX 8 (NEW): Eliminated 4× COUNT(DISTINCT user_key) in enrollment_agg
# + 1× in certificate_agg. Each COUNT(DISTINCT) forced a separate Expand
# pass; stacking 4 in one SELECT is exponentially bad on a 100M+ row scan.
# Rewrite as two-stage:
#   user_summary: dedupe to 1 row per (course_edition_key, org_key,
#     user_key) — cheap single GROUP BY, no Expand — carrying per-user
#     "ever_enrolled" / "ever_unenrolled" boolean flags via MAX(CASE).
#   enrollment_agg: plain SUM(flag) / COUNT(*) over user_summary. No
#     DISTINCT, no Expand.
# Semantics are identical:
#   - COUNT(DISTINCT user_key)                  == COUNT(*) over user_summary
#   - COUNT(DISTINCT CASE WHEN is_enrolled...)  == SUM(ever_enrolled)
#   - NULL is_enrolled → MAX(CASE…) returns 0 (unchanged: original
#     CASE WHEN is_enrolled also drops NULLs)
# ────────────────────────────────────────────────────────────
def _fact_conclusion_rate_agg_sql(tgt_layer: str) -> str:
    # FIX: Aggregate by NATURAL keys (display_number + edition + org_cd), not
    # SCD2 surrogates (course_edition_key + org_key). When dim_course_edition
    # has multiple SCD2 versions for the same course (e.g. attribute changed
    # mid-stream), enrollments and certificates land on different surrogate
    # keys depending on the event_ts. Grouping by surrogate splits a single
    # logical course into multiple agg rows, each with a partial slice of
    # users — producing impossible rates >100% for some slices.
    # ce_all_versions / org_all_versions map every SCD2 version to its
    # natural key; ce_current / org_current pick the latest version's
    # display info for the final projection.
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        org_all_versions AS (
            SELECT org_key, org_cd
            FROM {tgt_layer}.entidades.dim_organization
        ),
        user_all_versions AS (
            SELECT user_key, user_cd
            FROM {tgt_layer}.entidades.dim_user
        ),
        ce_current AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name,
                   MAX_BY(start_date,   key_start_date) AS start_date,
                   MAX_BY(end_date,     key_start_date) AS end_date
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        org_current AS (
            SELECT org_cd,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_cd
        ),

        enrollment_user_summary AS (
            SELECT
                ce.course_cd,
                ce.edition,
                org.org_cd,
                u.user_cd,
                MAX(CASE WHEN fce.is_enrolled     THEN 1 ELSE 0 END)  AS ever_enrolled,
                MAX(CASE WHEN NOT fce.is_enrolled THEN 1 ELSE 0 END)  AS ever_unenrolled
            FROM       {tgt_layer}.entidades.fact_course_enrollment_daily  fce
            JOIN       ce_all_versions   ce  ON fce.course_edition_key = ce.course_edition_key
            JOIN       org_all_versions  org ON fce.org_key            = org.org_key
            JOIN       user_all_versions u   ON fce.user_key           = u.user_key
            GROUP BY ce.course_cd, ce.edition, org.org_cd, u.user_cd
        ),

        enrollment_agg AS (
            SELECT
                course_cd,
                edition,
                org_cd,
                COUNT(*)              AS total_enrolled,
                SUM(ever_unenrolled)  AS total_unenrolled,
                SUM(ever_enrolled)    AS net_enrolled
            FROM enrollment_user_summary
            GROUP BY course_cd, edition, org_cd
        ),

        certificate_user_summary AS (
            SELECT DISTINCT
                ce.course_cd,
                ce.edition,
                org.org_cd,
                u.user_cd
            FROM       {tgt_layer}.entidades.fact_certificate_daily fc
            JOIN       ce_all_versions   ce  ON fc.course_edition_key = ce.course_edition_key
            JOIN       org_all_versions  org ON fc.org_key            = org.org_key
            JOIN       user_all_versions u   ON fc.user_key           = u.user_key
            WHERE fc.status = 'downloadable'
        ),

        certificate_agg AS (
            SELECT
                course_cd,
                edition,
                org_cd,
                COUNT(*) AS total_certificates
            FROM certificate_user_summary
            GROUP BY course_cd, edition, org_cd
        )

        SELECT /*+ BROADCAST(ce_current, org_current) */
            ea.org_cd,
            org_curr.org_short_name,
            ea.course_cd,
            ce_curr.course_name,
            ea.edition,
            ce_curr.start_date,
            ce_curr.end_date,
            ce_curr.end_date < current_timestamp()                              AS course_ended,
            ea.total_enrolled,
            ea.total_unenrolled,
            ea.net_enrolled,
            coalesce(ca.total_certificates, 0)                                  AS total_certificates,
            round(
                coalesce(ca.total_certificates, 0) * 100.0 / nullif(ea.net_enrolled, 0),
            2)                                                                  AS conclusion_rate_pct
        FROM enrollment_agg ea
        LEFT JOIN certificate_agg ca
            ON  ea.course_cd = ca.course_cd
            AND ea.edition   = ca.edition
            AND ea.org_cd    = ca.org_cd
        LEFT JOIN ce_current ce_curr
            ON  ea.course_cd = ce_curr.course_cd
            AND ea.edition   = ce_curr.edition
        LEFT JOIN org_current org_curr
            ON  ea.org_cd = org_curr.org_cd
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Tickets vs Cursos
#
# FIX: dim_course_edition consolidada por chave natural antes do join.
# Sem isto, múltiplas versões SCD2 de uma mesma (course_cd, edition)
# produziriam display_names diferentes (quebra filtros Superset) e
# corromperiam o cálculo de is_latest_edition — start_date é escolhido
# por MAX_BY(start_date, key_start_date) por (course_cd, edition) antes
# de se determinar a edição mais recente do curso.
# ────────────────────────────────────────────────────────────
def _tickets_vs_courses_agg_sql(tgt_layer: str) -> str:
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        ce_per_edition AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name,
                   MAX_BY(start_date,   key_start_date) AS start_date
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        ce_latest_per_course AS (
            SELECT course_cd,
                   edition,
                   course_name,
                   start_date,
                   CASE
                       WHEN start_date = MAX(start_date) OVER (PARTITION BY course_cd)
                       THEN 1 ELSE 0
                   END AS is_latest_edition
            FROM ce_per_edition
        ),
        org_current AS (
            SELECT org_key,
                   MAX_BY(org_cd,     key_start_date) AS org_cd,
                   MAX_BY(name,       key_start_date) AS org_name,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_key
        )
        SELECT /*+ BROADCAST(ce_latest_per_course, org_current) */
            fce.day_key,
            org_curr.org_cd,
            org_curr.org_name,
            org_curr.org_short_name,
            fce.course_cd,
            ce_latest.course_name,
            ce_latest.edition,
            ce_latest.is_latest_edition,
            CASE WHEN ce_latest.is_latest_edition = 1
                 THEN 'Novos Cursos' ELSE 'Reedições'
            END                                                             AS edition_type,
            t.ticket_type_origin,
            t.ticket_key
        FROM       {tgt_layer}.entidades.fact_course_edition_daily fce
        LEFT JOIN  org_current org_curr
                ON fce.org_key = org_curr.org_key
        LEFT JOIN  ce_all_versions ce
                ON fce.course_edition_key = ce.course_edition_key
        LEFT JOIN  ce_latest_per_course ce_latest
                ON ce.course_cd = ce_latest.course_cd
               AND ce.edition   = ce_latest.edition
        LEFT JOIN (
            SELECT
                DATE(created)       AS ticket_date,
                ticket_type_origin,
                key                 AS ticket_key
            FROM {tgt_layer}.gestao.jira_tickets
        ) t ON fce.day_key = t.ticket_date
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Certificates
# Pre-aggregated to (day_key, org, course, edition, month) grain.
# Superset metrics: SUM(certificate_count)
#
# FIX: Agrupado por chaves naturais (display_number + edition + org_cd) e
# display_name escolhido com MAX_BY(.., key_start_date) — mesma estratégia
# já aplicada em fact_conclusion_rate_agg. Sem isto, qualquer mudança de
# display_name (incluindo whitespace invisível) entre versões SCD2 parte
# o curso em múltiplas linhas no agg, e um filtro Superset por course_name
# captura só uma das versões — KPIs sub-contagem.
# ────────────────────────────────────────────────────────────
def _certificates_agg_sql(tgt_layer: str) -> str:
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        org_all_versions AS (
            SELECT org_key, org_cd
            FROM {tgt_layer}.entidades.dim_organization
        ),
        ce_current AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        org_current AS (
            SELECT org_cd,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_cd
        )
        SELECT /*+ BROADCAST(ce_current, org_current) */
            fc.day_key,
            org.org_cd,
            org_curr.org_short_name,
            ce.course_cd,
            ce_curr.course_name,
            ce.edition,
            CONCAT(
                CAST(dt.year AS STRING),
                LPAD(CAST(dt.month AS STRING), 2, '0'),
                ' - ',
                CAST(dt.month_name AS STRING)
            )                                           AS month_name,
            COUNT(DISTINCT fc.certificate_cd)           AS certificate_count
        FROM {tgt_layer}.entidades.fact_certificate_daily fc
        JOIN       ce_all_versions   ce       ON fc.course_edition_key = ce.course_edition_key
        JOIN       org_all_versions  org      ON fc.org_key            = org.org_key
        LEFT JOIN  ce_current        ce_curr  ON ce.course_cd          = ce_curr.course_cd
                                              AND ce.edition           = ce_curr.edition
        LEFT JOIN  org_current       org_curr ON org.org_cd            = org_curr.org_cd
        JOIN       {tgt_layer}.entidades.dim_time dt
                                              ON fc.day_key            = dt.date
        WHERE fc.status = 'downloadable'
        GROUP BY
            fc.day_key,
            org.org_cd,
            org_curr.org_short_name,
            ce.course_cd,
            ce_curr.course_name,
            ce.edition,
            dt.year,
            dt.month,
            dt.month_name
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Inscrições vs Certificados
# Pre-aggregated with SUM(total_days_to_conclusion) so Superset can compute
# AVG as SUM(total_days_to_conclusion) / SUM(certificate_count).
#
# FIX: Agrupado por chaves naturais. Ver comentário em _certificates_agg_sql.
# ────────────────────────────────────────────────────────────
def _enrollments_vs_certificates_agg_sql(tgt_layer: str) -> str:
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        org_all_versions AS (
            SELECT org_key, org_cd
            FROM {tgt_layer}.entidades.dim_organization
        ),
        ce_current AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        org_current AS (
            SELECT org_cd,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_cd
        )
        SELECT /*+ BROADCAST(ce_current, org_current) */
            fce.day_key,
            org.org_cd,
            org_curr.org_short_name,
            ce.course_cd,
            ce_curr.course_name,
            ce.edition,
            CONCAT(
                CAST(dt.year AS STRING),
                ' - ',
                CAST(dt.month_name AS STRING)
            )                                                           AS month_name,
            CASE
                WHEN fc.certificate_cd IS NOT NULL THEN 'concluded'
                ELSE 'enrolled'
            END                                                         AS status,
            COUNT(DISTINCT fce.course_enrollment_cd)                    AS enrollment_count,
            COUNT(DISTINCT fc.certificate_cd)                           AS certificate_count,
            SUM(CASE
                WHEN fc.certificate_cd IS NOT NULL
                THEN date_diff(DAY, fce.course_enrollment_start_date, fc.certificate_issue_date)
                ELSE 0
            END)                                                        AS total_days_to_conclusion
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily fce
        JOIN       ce_all_versions   ce       ON fce.course_edition_key = ce.course_edition_key
        JOIN       org_all_versions  org      ON fce.org_key            = org.org_key
        LEFT JOIN  ce_current        ce_curr  ON ce.course_cd           = ce_curr.course_cd
                                              AND ce.edition            = ce_curr.edition
        LEFT JOIN  org_current       org_curr ON org.org_cd             = org_curr.org_cd
        JOIN       {tgt_layer}.entidades.dim_time dt
                                              ON fce.day_key            = dt.date
        LEFT JOIN  {tgt_layer}.entidades.fact_certificate_daily fc
                                              ON fce.day_key            = fc.day_key
                                             AND fce.course_edition_key = fc.course_edition_key
                                             AND fce.user_key           = fc.user_key
                                             AND fce.org_key            = fc.org_key
                                             AND fc.status              = 'downloadable'
        GROUP BY
            fce.day_key,
            org.org_cd,
            org_curr.org_short_name,
            ce.course_cd,
            ce_curr.course_name,
            ce.edition,
            dt.year,
            dt.month_name,
            fc.certificate_cd
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Enrollment Flow & Active Students
# Grain: (day_key, org_cd, course_cd, edition, month_name)
#
# Serves 4 KPIs that the existing agg tables cannot answer:
#   1. new_enrollments  — flow: count of new enrollment events on day_key
#      (day_key = DATE(course_enrollment_start_date))
#   2. new_unenrollments — flow: count of explicit unenroll events on day_key
#      (day_key = DATE(unenrollment_date), only when history_type='delete')
#   3. active_students  — stock: distinct users enrolled on day_key, per
#      (org, course, edition). NOTE: do NOT sum across courses for the same
#      org to derive org-level unique students — a user in N courses counts N
#      times. For org-level unique students, filter to a single course or use
#      the raw fact_course_enrollment_daily.
#   4. Cumulative KPIs can be derived in Superset:
#      - SUM(new_enrollments)   over any date range → enrollments in period
#      - SUM(new_unenrollments) over any date range → unenrollments in period
#      - MAX(active_students) or filter day_key=X  → stock on a given day
#
# Two-stage aggregation (base → dedup → final) eliminates duplicate rows
# introduced by SCD2 joins on dim_course_edition / dim_organization before
# the final COUNT(DISTINCT user_key).
#
# Superset metrics: SUM(new_enrollments), SUM(new_unenrollments),
#                   SUM(active_students) [per course], MAX(active_students) [stock]
# ────────────────────────────────────────────────────────────
def _enrollment_flow_agg_sql(tgt_layer: str, day_filter_sql: str = "") -> str:
    # FIX: chaves naturais + MAX_BY(display_name) — ver _certificates_agg_sql.
    #
    # FIX (is_new_enrollment / is_new_unenrollment): comparar com
    # course_enrollment_start_date / unenrollment_date falha quando o aluno
    # se inscreve antes do start_date do curso — a expansão diária arranca
    # em ce_start (fact_course_enrollment_d.effective_start =
    # greatest(st_aluno, ce_start)) e o primeiro day_key na fact não bate
    # com a data de criação da inscrição. Marcamos new_enrollment no
    # primeiro day_key observado por course_enrollment_cd, e
    # new_unenrollment no último (apenas se unenrollment_date estiver
    # definida — protege contra cursos que apenas terminaram).
    # A CTE enrolment_bounds faz scan global do fact (sem o day_filter)
    # para não ser corrompida pelo pushdown_day_filter.
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        org_all_versions AS (
            SELECT org_key, org_cd
            FROM {tgt_layer}.entidades.dim_organization
        ),
        ce_current AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        org_current AS (
            SELECT org_cd,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_cd
        ),
        enrolment_bounds AS (
            SELECT
                course_enrollment_cd,
                MIN(day_key) AS first_day_in_fact,
                MAX(day_key) AS last_day_in_fact
            FROM {tgt_layer}.entidades.fact_course_enrollment_daily
            GROUP BY course_enrollment_cd
        ),
        base AS (
            SELECT /*+ BROADCAST(ce_current, org_current, dt) */
                fce.day_key,
                org.org_cd,
                org_curr.org_short_name,
                ce.course_cd,
                ce_curr.course_name,
                ce.edition,
                CONCAT(
                    CAST(dt.year  AS STRING),
                    LPAD(CAST(dt.month AS STRING), 2, '0'),
                    ' - ',
                    dt.month_name
                )                                                           AS month_name,
                fce.user_key,
                fce.course_enrollment_cd,
                fce.is_enrolled,
                CASE
                    WHEN fce.day_key = bounds.first_day_in_fact
                    THEN 1 ELSE 0
                END                                                         AS is_new_enrollment,
                CASE
                    WHEN fce.unenrollment_date IS NOT NULL
                     AND fce.day_key = bounds.last_day_in_fact
                    THEN 1 ELSE 0
                END                                                         AS is_new_unenrollment
            FROM       {tgt_layer}.entidades.fact_course_enrollment_daily   fce
            JOIN       ce_all_versions   ce       ON fce.course_edition_key = ce.course_edition_key
            JOIN       org_all_versions  org      ON fce.org_key            = org.org_key
            JOIN       enrolment_bounds  bounds   ON fce.course_enrollment_cd = bounds.course_enrollment_cd
            LEFT JOIN  ce_current        ce_curr  ON ce.course_cd           = ce_curr.course_cd
                                                  AND ce.edition            = ce_curr.edition
            LEFT JOIN  org_current       org_curr ON org.org_cd             = org_curr.org_cd
            JOIN       {tgt_layer}.entidades.dim_time dt
                                                  ON fce.day_key            = dt.date
            WHERE 1=1
              {day_filter_sql}
        ),
        dedup AS (
            SELECT
                day_key, org_cd, org_short_name, course_cd, course_name,
                edition, month_name, user_key, course_enrollment_cd,
                is_enrolled, is_new_enrollment, is_new_unenrollment
            FROM base
            GROUP BY
                day_key, org_cd, org_short_name, course_cd, course_name,
                edition, month_name, user_key, course_enrollment_cd,
                is_enrolled, is_new_enrollment, is_new_unenrollment
        )
        SELECT
            day_key,
            org_cd,
            org_short_name,
            course_cd,
            course_name,
            edition,
            month_name,
            SUM(is_new_enrollment)                                          AS new_enrollments,
            SUM(is_new_unenrollment)                                        AS new_unenrollments,
            COUNT(DISTINCT CASE WHEN is_enrolled THEN user_key END)         AS active_students
        FROM dedup
        GROUP BY
            day_key, org_cd, org_short_name, course_cd, course_name, edition, month_name
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Formandos únicos por (org, curso, edição) — user grain
# Grain: (org_cd, course_cd, edition, user_key)
#
# One row per unique enrollment of a user in a course edition. Collapses
# the daily snapshot of fact_course_enrollment_daily (N rows per user per
# edition, one per day) into a single user-grain row, so distinct-user
# counts become additive aggregations (COUNT(*) under filters).
#
# Solves two KPIs with minimal query cost:
#   KPI 1 (no time filter) — "Nº formandos por curso/entidade/edição":
#     SELECT COUNT(*) FROM ... WHERE org_cd = ? AND course_cd = ? AND edition = ?
#     SELECT COUNT(DISTINCT user_key) FROM ... WHERE org_cd = ?   -- cross-course
#
#   KPI 2 (time-range + currently enrolled) — approx via date bounds:
#     SELECT COUNT(DISTINCT user_key)
#     FROM ...
#     WHERE org_cd = ? AND course_cd = ?
#       AND first_enrollment_date <= '<end>'
#       AND last_active_date      >= '<start>'
#       AND is_currently_enrolled = true
#
# For exact distinct-user over an arbitrary range, query
# fact_course_enrollment_daily directly with day_key + is_enrolled filters.
#
# Full refresh: user-grain carries no day_key partition, and flags like
# is_currently_enrolled can flip for existing users on any incremental
# run, so partition-level overwrite isn't viable.
# ────────────────────────────────────────────────────────────
def _enrollment_users_agg_sql(tgt_layer: str) -> str:
    # FIX: agrega por chaves naturais (course_cd, edition, org_cd, user_cd) em
    # vez de surrogates SCD2 — consolida múltiplas versões SCD2 num único
    # registo user-grain. Sem isto, um user re-aparece em N linhas se a dim
    # do curso ou do user tiver versões diferentes ao longo do tempo, e
    # filtros Superset por course_name capturam apenas uma das versões.
    # user_key projectado mostra a versão MAX_BY(key_start_date) — útil para
    # joins ad-hoc mas o grão real é por user_cd natural.
    return f"""
        WITH ce_all_versions AS (
            SELECT course_edition_key,
                   display_number AS course_cd,
                   edition
            FROM {tgt_layer}.entidades.dim_course_edition
        ),
        org_all_versions AS (
            SELECT org_key, org_cd
            FROM {tgt_layer}.entidades.dim_organization
        ),
        user_all_versions AS (
            SELECT user_key, user_cd
            FROM {tgt_layer}.entidades.dim_user
        ),
        ce_current AS (
            SELECT display_number AS course_cd,
                   edition,
                   MAX_BY(display_name, key_start_date) AS course_name
            FROM {tgt_layer}.entidades.dim_course_edition
            GROUP BY display_number, edition
        ),
        org_current AS (
            SELECT org_cd,
                   MAX_BY(short_name, key_start_date) AS org_short_name
            FROM {tgt_layer}.entidades.dim_organization
            GROUP BY org_cd
        ),
        user_current AS (
            SELECT user_cd,
                   MAX_BY(user_key, key_start_date) AS user_key
            FROM {tgt_layer}.entidades.dim_user
            GROUP BY user_cd
        ),
        fact_with_natural_keys AS (
            SELECT
                ce.course_cd,
                ce.edition,
                org.org_cd,
                u.user_cd,
                fce.day_key,
                fce.is_enrolled
            FROM       {tgt_layer}.entidades.fact_course_enrollment_daily fce
            JOIN       ce_all_versions   ce  ON fce.course_edition_key = ce.course_edition_key
            JOIN       org_all_versions  org ON fce.org_key            = org.org_key
            JOIN       user_all_versions u   ON fce.user_key           = u.user_key
        ),
        user_edition_agg AS (
            SELECT
                course_cd,
                edition,
                org_cd,
                user_cd,
                MIN(day_key)                                          AS first_enrollment_date,
                MAX(CASE WHEN is_enrolled     THEN day_key END)       AS last_active_date,
                MAX(CASE WHEN NOT is_enrolled THEN day_key END)       AS last_unenrollment_date,
                MAX_BY(is_enrolled, day_key)                          AS is_currently_enrolled
            FROM fact_with_natural_keys
            GROUP BY course_cd, edition, org_cd, user_cd
        )
        SELECT /*+ BROADCAST(ce_current, org_current, user_current) */
            uea.org_cd,
            org_curr.org_short_name,
            uea.course_cd,
            ce_curr.course_name,
            uea.edition,
            u_curr.user_key,
            uea.user_cd,
            uea.first_enrollment_date,
            uea.last_active_date,
            uea.last_unenrollment_date,
            uea.is_currently_enrolled
        FROM       user_edition_agg uea
        LEFT JOIN  ce_current       ce_curr  ON uea.course_cd = ce_curr.course_cd
                                            AND uea.edition   = ce_curr.edition
        LEFT JOIN  org_current      org_curr ON uea.org_cd    = org_curr.org_cd
        LEFT JOIN  user_current     u_curr   ON uea.user_cd   = u_curr.user_cd
    """


# ────────────────────────────────────────────────────────────
# Registry
# output_partitions tuned per table based on expected output size:
#   - fact_conclusion_rate_agg:   now aggregated, small → 10 partitions
#   - tickets_vs_courses_agg:     small → 5 partitions
#   - certificates_agg:           small → 10 partitions
#   - enrollments_vs_certificates_agg: medium → 20 partitions
#   - enrollment_flow_agg:        medium, daily flow + stock → 20 partitions
#   - enrollment_users_agg:       user grain per edition, medium → 20 partitions
# ────────────────────────────────────────────────────────────
AGG_TABLES: list[AggTable] = [
    AggTable(
        name               = "fact_conclusion_rate_agg",
        sql_fn             = _fact_conclusion_rate_agg_sql,
        partition_by       = "org_cd",
        sort_order         = "org_cd ASC, course_cd ASC, edition ASC",
        output_partitions  = 10,
        full_refresh       = True,
    ),
    AggTable(
        name               = "tickets_vs_courses_agg",
        sql_fn             = _tickets_vs_courses_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, course_cd ASC, ticket_type_origin ASC",
        output_partitions  = 5,
    ),
    AggTable(
        name               = "certificates_agg",
        sql_fn             = _certificates_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 10,
    ),
    AggTable(
        name               = "enrollments_vs_certificates_agg",
        sql_fn             = _enrollments_vs_certificates_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 20,
    ),
    AggTable(
        name                 = "enrollment_flow_agg",
        sql_fn               = _enrollment_flow_agg_sql,
        partition_by         = "days(day_key)",
        sort_order           = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions    = 20,
        pushdown_day_filter  = True,
    ),
    AggTable(
        name               = "enrollment_users_agg",
        sql_fn             = _enrollment_users_agg_sql,
        partition_by       = "org_cd",
        sort_order         = "org_cd ASC, course_cd ASC, edition ASC, user_key ASC",
        output_partitions  = 20,
        full_refresh       = True,
    ),
]


# ============================================================
# Core rebuild helpers
# ============================================================

def _ensure_table_exists(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> bool:
    """
    Create the table via CTAS if it doesn't already exist.
    FIX 1+2: Uses repartition(output_partitions) before writing to control
    the number of output files written to S3, avoiding the 2,675-file problem
    caused by shuffle partitions × partition keys.
    Returns True if the table was just created (first run).
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {tgt_layer}.{pipeline}").collect()]
    if agg.name in tables:
        return False

    logging.info(f"First run: creating {full_name} (output_partitions={agg.output_partitions})…")

    # Create empty table first with correct schema and properties
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name}
        USING iceberg
        PARTITIONED BY ({agg.partition_by})
        TBLPROPERTIES (
            'write.parquet.compression-codec'            = 'zstd',
            'write.target-file-size-bytes'               = '536870912',
            'write.distribution-mode'                    = 'hash',
            'write.sort.order'                           = '{agg.sort_order}',
            'read.split.target-size'                     = '134217728',
            'read.split.open-file-cost'                  = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
        AS SELECT * FROM ({agg.sql_fn(tgt_layer)}) _empty WHERE 1=0
    """)

    # Write data with controlled partition count to limit S3 file count
    (
        spark.sql(agg.sql_fn(tgt_layer))
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .option("overwrite-mode", "dynamic")
             .overwritePartitions()
    )

    logging.info(f"Table {full_name} created (first run).")
    return True


def _overwrite_changed_partitions(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> None:
    """
    Recompute and overwrite only the day_key partitions that changed
    since last_execution_timestamp.
    FIX 1+2: Uses repartition(output_partitions) to control file count.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    changed_days_df = spark.sql(f"""
        SELECT DISTINCT day_key
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily
        WHERE last_update_timestamp > TIMESTAMP '{last_execution_timestamp}'
        UNION
        SELECT DISTINCT CAST(day_key AS DATE) AS day_key
        FROM {tgt_layer}.entidades.fact_certificate_daily
        WHERE last_update_timestamp > TIMESTAMP '{last_execution_timestamp}'
    """)

    changed_days_df.createOrReplaceTempView("_changed_days")

    changed_day_count = changed_days_df.count()
    if changed_day_count == 0:
        logging.info(f"No changed partitions detected for {full_name}, skipping overwrite.")
        return

    logging.info(
        f"Overwriting {changed_day_count} changed day_key partition(s) in {full_name} "
        f"(output_partitions={agg.output_partitions}, "
        f"pushdown_day_filter={agg.pushdown_day_filter})…"
    )

    # FIX 8: When the sql_fn supports it, push the day_key filter into the
    # fact-table scan BEFORE joins + aggregation.  The outer INNER JOIN is
    # kept regardless as a correctness safety belt — dynamic overwrite can
    # only touch partitions present in _changed_days, even if the inline
    # filter were ever buggy.
    if agg.pushdown_day_filter:
        day_filter_sql = "AND fce.day_key IN (SELECT day_key FROM _changed_days)"
        base_sql = agg.sql_fn(tgt_layer, day_filter_sql)
    else:
        base_sql = agg.sql_fn(tgt_layer)

    incremental_sql = f"""
        SELECT agg.*
        FROM (
            {base_sql}
        ) agg
        INNER JOIN _changed_days cd ON agg.day_key = cd.day_key
    """

    (
        spark.sql(incremental_sql)
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .option("overwrite-mode", "dynamic")
             .overwritePartitions()
    )

    logging.info(f"Partition overwrite complete for {full_name}.")


def _run_iceberg_maintenance(spark, tgt_layer: str, full_name: str, sort_order: str) -> None:
    try:
        spark.sql(f"""
            CALL {tgt_layer}.system.rewrite_data_files(
                table      => '{full_name}',
                strategy   => 'sort',
                sort_order => '{sort_order}',
                options    => map(
                    'min-input-files',                    '2',
                    'rewrite-all',                        'false',
                    'max-concurrent-file-group-rewrites', '20',
                    'partial-progress.enabled',           'true',
                    'partial-progress.max-commits',       '10'
                )
            )
        """)
        spark.sql(f"CALL {tgt_layer}.system.rewrite_manifests(table => '{full_name}')")
        spark.sql(f"CALL {tgt_layer}.system.expire_snapshots(table => '{full_name}', retain_last => 5)")
        logging.info(f"Iceberg maintenance completed for {full_name}.")
    except Exception as e:
        logging.warning(f"Iceberg maintenance skipped for {full_name}: {e}")


def _get_row_count_from_metadata(spark, tgt_layer: str, full_name: str) -> int:
    try:
        row_count = int(spark.sql(f"""
            SELECT summary['total-records']
            FROM {tgt_layer}.entidades.snapshots
            WHERE table_name = '{full_name}'
            ORDER BY committed_at DESC
            LIMIT 1
        """).first()[0])
    except Exception:
        try:
            row_count = int(spark.sql(f"""
                SELECT snapshot_summary['total-records']
                FROM {full_name}.history
                ORDER BY made_current_at DESC
                LIMIT 1
            """).first()[0])
        except Exception:
            logging.warning("Could not read row count from Iceberg metadata; defaulting to -1.")
            row_count = -1
    return row_count


def _full_refresh_table(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> None:
    """
    Full replace for tables without a day_key grain (full_refresh=True).
    Truncates then rewrites all data in a single pass.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"
    logging.info(f"Full refresh: truncating and rewriting {full_name}…")
    spark.sql(f"TRUNCATE TABLE {full_name}")
    (
        spark.sql(agg.sql_fn(tgt_layer))
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .append()
    )
    logging.info(f"Full refresh complete for {full_name}.")


def _rebuild_agg_table(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> int:
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    first_run = _ensure_table_exists(spark, tgt_layer, pipeline, agg)

    if not first_run:
        if agg.full_refresh:
            _full_refresh_table(spark, tgt_layer, pipeline, agg)
        else:
            _overwrite_changed_partitions(spark, tgt_layer, pipeline, agg, last_execution_timestamp)

    # FIX 7 (NEW): Skip Iceberg maintenance on first run.
    # On a freshly written table the files are already optimally sized from
    # repartition(), so compaction is wasted work — it adds 2-5 min per table
    # and competes with still-running writes from other parallel threads.
    if not first_run:
        _run_iceberg_maintenance(spark, tgt_layer, full_name, agg.sort_order)
    else:
        logging.info(
            f"Skipping Iceberg maintenance for {full_name} (first run — files already optimal)."
        )

    row_count = _get_row_count_from_metadata(spark, tgt_layer, full_name)
    logging.info(f"Table {full_name} has {row_count:,} total rows.")

    return row_count


# ============================================================
# Entry point
# ============================================================

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_reporting_agg_tables")

    # FIX 1: Reduced shuffle.partitions from 200 → 50.
    # 200 was creating too many output files (2,675 per table) causing slow
    # S3 writes. AQE will coalesce small partitions automatically.
    # The repartition() calls in _ensure_table_exists and
    # _overwrite_changed_partitions further control the final file count.
    spark.conf.set("spark.sql.shuffle.partitions", "50")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # FIX 1: Tune AQE coalescing to target ~512MB files.
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "536870912")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "134217728")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    tgt_layer    = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"

    # FIX 9: Cache the two hottest fact tables so the aggregation queries
    # can reuse a single Iceberg scan instead of re-reading Parquet from S3.
    # fact_course_enrollment_daily is scanned by 4 of 6 queries;
    # fact_certificate_daily is scanned by 3 of 6.
    # CACHE LAZY TABLE = MEMORY_AND_DISK storage level by default — Spark
    # spills to disk under memory pressure rather than OOMing executors.
    #
    # Trade-off note: once cached, Spark serves reads from the in-memory
    # relation instead of going back to Iceberg. This means Iceberg
    # partition-metadata pruning (e.g. the days(day_key) partition scheme)
    # is bypassed; a pushdown filter now filters rows in-memory rather than
    # pruning Parquet files before read. For a pipeline dominated by
    # full-table scans (first run, full_refresh, queries that do not use
    # pushdown_day_filter) this is a clear win. If your day-to-day runs
    # become dominated by very narrow incremental windows on
    # enrollment_flow_agg (e.g. 1–2 changed days), disable this:
    #   CACHE_FACT_TABLES=false
    cache_fact_tables = os.environ.get("CACHE_FACT_TABLES", "true").lower() == "true"
    if cache_fact_tables:
        logging.info("Caching fact tables (CACHE_FACT_TABLES=true)…")
        spark.sql(f"CACHE LAZY TABLE {tgt_layer}.entidades.fact_course_enrollment_daily")
        spark.sql(f"CACHE LAZY TABLE {tgt_layer}.entidades.fact_certificate_daily")
    else:
        logging.info("Fact-table caching disabled (CACHE_FACT_TABLES=false).")

    tables_to_run_env = os.environ.get("TABLES_TO_RUN", "").strip()
    if tables_to_run_env:
        requested = {t.strip() for t in tables_to_run_env.split(",")}
        valid_names = {agg.name for agg in AGG_TABLES}
        unknown = requested - valid_names
        if unknown:
            raise ValueError(
                f"Unknown table(s) in TABLES_TO_RUN: {unknown}. "
                f"Valid options are: {valid_names}"
            )
        tables = [agg for agg in AGG_TABLES if agg.name in requested]
        logging.info(f"TABLES_TO_RUN is set — processing {len(tables)} table(s): {requested}")
    else:
        tables = AGG_TABLES
        logging.info(f"TABLES_TO_RUN not set — processing all {len(tables)} table(s).")

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # FIX 4 (UPDATED): Default max_workers lowered from 3 → 1.
    # Running heavy tables in parallel on a shared Spark session causes
    # executor OOM kills and cascading shuffle fetch failures when multiple
    # large shuffles compete for the same executor memory.  Sequential
    # execution (max_workers=1) is safer for first-run / backfill.
    # For incremental runs where only a few partitions change per table,
    # set AGG_MAX_WORKERS=3 via env var to re-enable parallelism.
    max_workers = int(os.environ.get("AGG_MAX_WORKERS", "1"))
    logging.info(f"Running {len(tables)} table(s) with max_workers={max_workers}.")

    results: dict[str, int] = {}
    errors: dict[str, Exception] = {}

    def _run(agg: AggTable) -> tuple[str, int]:
        last_execution_timestamp = get_max_timestamp_for_table(
            spark_session=spark, table_name=agg.name, env=ENVIRONMENT
        )
        logging.info(f"--- Starting rebuild: {agg.name} (last_exec={last_execution_timestamp}) ---")
        row_count = _rebuild_agg_table(spark, tgt_layer, tgt_pipeline, agg, last_execution_timestamp)
        logging.info(f"--- Finished: {agg.name} ---")
        return agg.name, row_count

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_agg = {executor.submit(_run, agg): agg for agg in tables}
        for future in as_completed(future_to_agg):
            agg = future_to_agg[future]
            try:
                name, row_count = future.result()
                results[name] = row_count
            except Exception as exc:
                logging.error(f"Table {agg.name} failed: {exc}", exc_info=True)
                errors[agg.name] = exc

    # Update control table sequentially after all tables complete
    for agg in tables:
        if agg.name in results:
            update_ctrl_table(
                spark_session     = spark,
                table_name        = agg.name,
                current_timestamp = current_timestamp,
                number_of_records = results[agg.name],
                env               = ENVIRONMENT,
            )

    if cache_fact_tables:
        try:
            spark.sql(f"UNCACHE TABLE IF EXISTS {tgt_layer}.entidades.fact_course_enrollment_daily")
            spark.sql(f"UNCACHE TABLE IF EXISTS {tgt_layer}.entidades.fact_certificate_daily")
        except Exception as e:
            logging.warning(f"UNCACHE TABLE failed (cache will be released at session end): {e}")

    if errors:
        failed = list(errors.keys())
        raise RuntimeError(
            f"{len(failed)} table(s) failed: {failed}. "
            f"Check logs above for details."
        )

    logging.info("All aggregation tables rebuilt successfully.")


if __name__ == "__main__":
    main()
