# PD — Entidades Externas (External Entities Data Product)

> **NAU Analytics** · Plataforma de cursos online (Open edX) · Domínio _Entidades_
>
> Repositório: `nau-analytics-external-data-product`
> Plataforma: PySpark + Apache Iceberg + S3 + Spark on Kubernetes (orquestrado por Airflow)
> Página Confluence: [PD - Entidades Externas](https://glinttdev.atlassian.net/wiki/spaces/FIDDP/pages/2080632274947/PD+-+Entidades+Externas)

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura Medallion](#2-arquitetura-medallion)
3. [Estrutura do Repositório](#3-estrutura-do-repositório)
4. [Convenções e Padrões Transversais](#4-convenções-e-padrões-transversais)
5. [Camada Bronze — Ingestão](#5-camada-bronze--ingestão)
6. [Camada Silver — Limpeza e Tipagem](#6-camada-silver--limpeza-e-tipagem)
7. [Camada Gold — Modelo Dimensional](#7-camada-gold--modelo-dimensional)
8. [Camada Gold/Reporting — Agregadas para Superset](#8-camada-goldreporting--agregadas-para-superset)
9. [Operação, Deploy e CI/CD](#9-operação-deploy-e-cicd)
10. [Variáveis de Ambiente](#10-variáveis-de-ambiente)
11. [Observações e Pontos de Atenção](#11-observações-e-pontos-de-atenção)

---

## 1. Visão Geral

Este repositório contém todo o código (PySpark + SQL embebido) que constrói o **data product das Entidades Externas** da plataforma NAU. O produto serve análises de:

- **Inscrições** em cursos por entidade/curso/edição;
- **Conclusões e certificados** emitidos;
- **Notas finais dos formandos**;
- **Atividade diária** das edições de curso e KPIs de fluxo (inscrições novas, desistências, alunos ativos).

Os dashboards finais são consumidos em **Apache Superset** sobre as tabelas agregadas `gold<env>.entidades.*_agg`.

> **Nota de organização:** os DAGs de orquestração Airflow vivem **noutro repositório** (gestão Kubernetes/cluster) — este repositório contém apenas as transformações de dados e a imagem Docker que as executa.

---

## 2. Arquitetura Medallion

A pipeline segue o padrão **Bronze / Silver / Gold** (Medallion) sobre **Apache Iceberg**, com object store S3-compatible para os ficheiros Parquet e catálogo Iceberg em MySQL.

### 2.1 Diagrama de arquitetura

> O diagrama drawio com a arquitetura completa encontra-se em [`docs/diagrams/01_medallion_architecture.drawio`](diagrams/01_medallion_architecture.drawio). Para o embeber no Confluence, use a macro **drawio** apontando para o ficheiro anexado à página, ou cole o XML diretamente como _diagram source_.

| Camada | Catálogo Iceberg | Função |
|---|---|---|
| **Source** | MySQL `edxapp` (Open edX LMS) | Sistema operacional de origem |
| **Bronze** | `bronze<env>.entidades` | Snapshot histórico das tabelas de origem (raw + metadata) |
| **Silver** | `silver<env>.entidades` | Limpeza, tipagem, extração de campos JSON, deduplicação leve |
| **Gold** | `gold<env>.entidades` | Modelo dimensional (Star Schema) com SCD2 nas dimensões |
| **Gold reporting** | `gold<env>.entidades.*_agg` | Tabelas agregadas pré-calculadas para Superset |

`<env>` é o sufixo do ambiente (ex.: `_dev`, `_prod`, `_local`) — definido pela variável de ambiente `ENVIRONMENT`. Em ambiente local, é típico ser `_local`, ficando a tabela `bronze_local.entidades.auth_user`.

### 2.2 Sistema de origem

Todas as tabelas bronze têm origem no **schema `edxapp`** do **Open edX** (a plataforma LMS subjacente), via JDBC MySQL. Tabelas envolvidas na fonte:

| Tabela origem (MySQL) | Domínio |
|---|---|
| `auth_user` | Identidade/autenticação do utilizador |
| `auth_userprofile` | Perfil demográfico (com campos JSON em `meta`) |
| `certificates_generatedcertificate` | Certificados emitidos |
| `course_overviews_courseoverview` | Definição da edição do curso |
| `grades_persistentcoursegrade` | Notas finais persistentes por curso |
| `organizations_organization` | Entidades/organizações ativas |
| `organizations_historicalorganization` | Histórico SCD da entidade (Django Simple History) |
| `student_courseaccessrole` | Papéis de acesso a cursos |
| `student_courseenrollment` | Inscrições atuais |
| `student_courseenrollment_history` | Histórico de inscrições/cancelamentos |
| `student_userattribute` | Atributos chave/valor por utilizador |

---

## 3. Estrutura do Repositório

```
root/
├── .github/
│   └── workflows/
│       └── docker-build-push.yml         # CI/CD: build & push da imagem Docker
├── Docker/
│   └── Dockerfile                         # Imagem PySpark com nau-analytics-utils
├── readme.md
└── src/
    ├── bronze/
    │   └── python/
    │       ├── bronze_*_ingestion.py     # 11 scripts de ingestão
    │       ├── misc/
    │       │   ├── get_full_tables.py    # Carga inicial completa (one-shot)
    │       │   ├── incremental_load.py   # Loader genérico legacy
    │       │   └── hello_spark.py        # Smoke test do cluster
    │       └── utils/
    │           └── bronze_utils_functions.py
    ├── silver/
    │   ├── silver_*.py                    # 9 scripts (1 por tabela bronze)
    │   └── utils/
    │       └── silver_utils_functions.py
    └── gold/
        ├── gold_dim_*.py                  # 4 dimensões
        ├── gold_fact_*.py                 # 4 factos
        ├── gold_reporting_agg_tables.py   # 6 datasets agregados (Superset)
        └── utils/
            └── gold_utils_functions.py
```

---

## 4. Convenções e Padrões Transversais

### 4.1 Nomenclatura

- **Tabelas bronze e silver** → mantêm o nome da tabela de origem MySQL (ex.: `auth_user`, `student_courseenrollment`).
- **Tabelas gold** → seguem o prefixo `dim_*` (dimensão) ou `fact_*` (facto). Tabelas agregadas para BI usam o sufixo `_agg`.
- **Surrogate keys SCD2** → sufixo `_key` (ex.: `user_key`, `org_key`, `course_edition_key`).
- **Business keys** → sufixo `_cd` (ex.: `user_cd`, `org_cd`, `course_edition_cd`).
- **Pipeline** → todas as tabelas vivem dentro do schema lógico `entidades` (ex.: `bronze_dev.entidades.auth_user`).

### 4.2 Tabela de auditoria — `pipeline_run_ctrl`

Existe uma tabela de auditoria por camada:

- `bronze<env>.audit.pipeline_run_ctrl`
- `silver<env>.audit.pipeline_run_ctrl`
- `gold<env>.audit.pipeline_run_ctrl`

Cada execução de um script faz `INSERT` com `(pipeline, table_name, last_execution_ts, number_of_records)`. Esta tabela serve de **marca de água (high-water mark)** para o próximo run incremental: o `get_max_timestamp_for_table()` lê o `MAX(last_execution_ts)` filtrado por `(pipeline='entidades', table_name=<x>)`. Se nunca foi executado, o default é `'1900-01-01 00:00:00'`, o que despoleta um **backfill completo**.

### 4.3 Helpers (utils)

Cada camada tem o seu módulo `*_utils_functions.py`:

| Função | Camada | Descrição |
|---|---|---|
| `add_ingestion_metadata_column(df, table, ts)` | bronze | Adiciona `ingestion_date` e `source_name` |
| `read_data_from_sql(spark, query, jdbc_url, user, secret)` | bronze | Leitura JDBC genérica do MySQL |
| `update_ctrl_table(spark, table_name, ts, n, env)` | bronze, silver, gold | Insere linha de auditoria |
| `get_max_timestamp_for_table(spark, table_name, env)` | bronze, silver, gold | Devolve a última marca de água da tabela |
| `get_delta_dataframe(tgt, src)` | bronze | Junta src ↔ tgt e devolve linhas novas/alteradas com base em `row_hash` |
| `validate_table_that_delete_lines(tgt, src)` | bronze | Alerta sobre `id` que existe em src mas não em tgt (atualmente apenas avisa, não aborta — ver §11) |
| `validate_ingestion_values(spark, src_df, table_name, env)` | bronze | Conta `id` distintos na target após o append |

### 4.4 Sessão Spark e Iceberg

A inicialização da sessão Spark é feita pela função `start_iceberg_session(<app_name>)` da biblioteca **`nau-analytics-utils`** (instalada via `pip install git+https://github.com/fccn/nau-analytics-utils.git@main#subdirectory=common_libs/utils`). Esta encapsula a configuração do catálogo Iceberg, credenciais S3 e configurações Spark padrão.

---

## 5. Camada Bronze — Ingestão

> Diagrama detalhado em [`docs/diagrams/03_bronze_ingestion_patterns.drawio`](diagrams/03_bronze_ingestion_patterns.drawio).

A camada bronze materializa o **estado bruto** das tabelas de origem do Open edX em formato Iceberg, preservando a auditabilidade (`ingestion_date`, `source_name`).

### 5.1 Padrões de ingestão usados

Existem **três padrões** de ingestão neste repositório:

#### Padrão A — CDC por hash (delta dataframe)

Usado em tabelas onde **não há campos `created`/`modified` fiáveis**, mas onde se quer captar `INSERT + UPDATE` evitando reprocessamento de linhas inalteradas.

**Tabelas:** `auth_user`, `auth_userprofile`, `student_courseaccessrole`, `student_courseenrollment`.

Lógica (resumida):

```sql
-- Na fonte MySQL:
SELECT id, col1, col2, ...,
       SHA1(CONCAT_WS('||', col1, col2, ...)) AS row_hash
FROM <src_table>
```

```python
# No Spark, junção delta:
w = Window.partitionBy("id").orderBy(F.col("ingestion_date").desc())
tgt_dedup = tgt.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

df_delta = (
    src.alias("s")
       .join(tgt_dedup.alias("t"), "s.id = t.id", "left")
       .where("t.id IS NULL OR s.row_hash != t.row_hash")
       .select("s.*")
)
df_delta.write.format("iceberg").mode("append").saveAsTable(...)
```

**Características:**

| | |
|---|---|
| Captura | INSERT + UPDATE |
| **Não** captura | DELETE (apenas alerta via `validate_table_that_delete_lines`) |
| Vantagem | Sem dependência de timestamps na fonte; fiável a UPDATE de qualquer coluna |
| Custo | Leitura completa da target a cada run (mitigado via dedup window) |

#### Padrão B — Janela incremental por `created`/`modified`

Usado em tabelas com timestamps fiáveis na fonte. A query JDBC já é filtrada na origem, minimizando o tráfego de rede.

**Tabelas:** `certificates_generatedcertificate`, `course_overviews_courseoverview`, `grades_persistentcoursegrade`, `organizations_organization`, `student_userattribute`, `student_courseenrollment_history`.

```python
start_date = get_max_timestamp_for_table(spark, table, env)  # ou '1900-01-01' se 1ª run

query = f"""(
    SELECT * FROM {src_table}
    WHERE created >= '{start_date}' OR modified >= '{start_date}'
) AS T1"""
```

A target é particionada por `days(ingestion_date)` para acelerar leituras temporais.

#### Padrão C — Snapshot único (one-shot)

Usado **exclusivamente** para `organizations_historicalorganization`. O script verifica se é a primeira execução (`start_date == '1900-01-01'`) — se não for, sai imediatamente. O objetivo é alimentar **uma única vez** o histórico SCD2 inicial de `dim_organization`.

```python
if start_date != FIXED_START_DATE:
    return  # já foi carregado anteriormente
```

### 5.2 Lista de tabelas bronze

| Script | Target | Padrão | Particionamento | Hash |
|---|---|---|---|---|
| `bronze_auth_user_ingestion.py` | `auth_user` | A (CDC hash) | — | ✅ |
| `bronze_auth_userprofile_ingestion.py` | `auth_userprofile` | A (CDC hash) | — | ✅ |
| `bronze_certificates_generatedcertificate_ingestion.py` | `certificates_generatedcertificate` | B (created/modified) | `days(ingestion_date)` | — |
| `bronze_course_overviews_courseoverview_ingestion.py` | `course_overviews_courseoverview` | B (created/modified) | `days(ingestion_date)` | — |
| `bronze_grades_persistentcoursegrade_ingestion.py` | `grades_persistentcoursegrade` | B (created/modified) | `days(ingestion_date)` | — |
| `bronze_organizations_ho_ingestion.py` | `organizations_historicalorganization` | C (one-shot) | — | — |
| `bronze_organizations_organization_ingestion.py` | `organizations_organization` | B (created/modified) | `days(ingestion_date)` | — |
| `bronze_student_courseaccessrole_ingestion.py` | `student_courseaccessrole` | A (CDC hash) | `days(ingestion_date)` | ✅ |
| `bronze_student_courseenrollment_ingestion.py` | `student_courseenrollment` | A (CDC hash) | `days(ingestion_date)` | ✅ |
| `bronze_student_courseenrollment_history_ingestion.py` | `student_courseenrollment_history` | B (LEAST(created, history_date)) | — | — |
| `bronze_student_userattribute_ingestion.py` | `student_userattribute` | B (created/modified) | — | — |

### 5.3 Scripts auxiliares (`src/bronze/python/misc/`)

- **`get_full_tables.py`** — utilitário CLI para fazer **carga inicial completa** de todas as tabelas (passando `--undesired_column` para drop opcional na `auth_user`, ex.: coluna `password`).
- **`incremental_load.py`** — loader genérico legado; assume metadata em CSV no S3 e suporta `full_initial_ingestion` (loop mensal desde 2020) ou `delta_load`. **Não está integrado** no fluxo regular dos scripts `bronze_*_ingestion.py`.
- **`hello_spark.py`** — smoke test mínimo para validar que o cluster Spark on Kubernetes está OK.

---

## 6. Camada Silver — Limpeza e Tipagem

A camada silver é uma **projeção tipada e limpa** do bronze. Cada script segue o mesmo padrão minimalista:

```python
last_execution_timestamp = get_max_timestamp_for_table(spark, tgt_table, env)

df_src = spark.sql(f"""
    SELECT <colunas selecionadas com cast/limpeza>
    FROM bronze<env>.entidades.<table>
    WHERE ingestion_date > '{last_execution_timestamp}'
""")

df_src.write.format("iceberg").mode("append").saveAsTable(f"silver<env>.entidades.<table>")
update_ctrl_table(...)
```

### 6.1 Transformações por tabela silver

| Tabela silver | Transformação aplicada vs. bronze |
|---|---|
| `auth_user` | Recalcula `row_hash` em Spark (consistência), copia colunas |
| `auth_userprofile` | **Extrai `employment_situation` de `meta` (JSON)** com `get_json_object(meta, '$.employment_situation')`; descarta colunas brutas raramente usadas (`courseware`, `mailing_address`, `goals`, `bio`, `profile_image_uploaded_at`) |
| `certificates_generatedcertificate` | Limpa colunas de uso interno (`verify_uuid`, `download_uuid`, `download_url`, `key`, `name`) |
| `course_overviews_courseoverview` | Aplica `GREATEST(start, '2015-01-01')` para evitar datas inválidas anteriores ao arranque da plataforma; remove URLs de imagem/social |
| `grades_persistentcoursegrade` | Cópia tipada |
| `organizations_organization` | Cópia tipada (sem `logo`) |
| `organizations_historicalorganization` | **Carga única** — só executa se `last_execution_ts == '1900-01-01'` (alimenta SCD2 inicial de `dim_organization`) |
| `student_courseenrollment` | **Full load** (sem filtro por `ingestion_date`) — porque na fonte os registos são deletados ao desinscrever (passam para a tabela history). Necessário recompor o estado total a cada run |
| `student_courseenrollment_history` | Filtro por `ingestion_date > last_exec` |

### 6.2 Pontos chave

- **Marca de água própria** — silver mantém a sua tabela `silver<env>.audit.pipeline_run_ctrl` separada da bronze.
- **Sem deduplicação SCD** — silver pode conter múltiplas versões da mesma chave; é a camada gold que aplica SCD2.
- **Particionamento removido** — em silver as tabelas não são particionadas (volumes mais pequenos após o filtro de janela).

---

## 7. Camada Gold — Modelo Dimensional

> Diagrama ERD em [`docs/diagrams/02_gold_layer_erd.drawio`](diagrams/02_gold_layer_erd.drawio).

A camada gold materializa o **modelo dimensional em estrela** que serve de fonte da verdade para reporting. Inclui **3 dimensões SCD2**, **1 dimensão estática** e **4 tabelas de factos**.

### 7.1 Mapa de tabelas gold

| Tabela | Tipo | Grão | SCD | Particionamento |
|---|---|---|---|---|
| `dim_user` | Dimensão | utilizador | SCD2 (`9999-12-31` = ativo) | — |
| `dim_organization` | Dimensão | entidade | SCD2 (`NULL` = ativo) | — |
| `dim_course_edition` | Dimensão | edição de curso | SCD2 (`NULL` = ativo) | — |
| `dim_time` | Dimensão | dia (1900–2100) | Estática (overwrite) | — |
| `fact_certificate_daily` | Facto | certificado × dia de emissão | — | — |
| `fact_course_enrollment_daily` | Facto | inscrição × dia | — | `days(day_key)` |
| `fact_course_edition_daily` | Facto | edição × dia ativo | — | `day_key` |
| `fact_student_grades` | Facto | nota final por aluno × edição | — | — |

### 7.2 Dimensões

#### `dim_time`

Dimensão estática gerada por `sequence(to_date('1900-01-01'), to_date('2100-12-31'), interval 1 day)` e enriquecida com atributos calendário (ano, trimestre, mês, semana ISO, dia da semana, _flags_ `is_weekend`/`is_month_start`/`is_quarter_end`/etc.) e feriados (atualmente apenas Ano Novo e Natal — pode ser estendido).

Chave primária: `time_key INT` no formato `yyyyMMdd` (ex.: `20260427`).

#### `dim_organization` — SCD Type 2

Construída a partir de `silver.organizations_organization` (regular) e `silver.organizations_historicalorganization` (Django simple-history) na primeira execução.

- **Surrogate key:** `org_key BIGINT = org_cd * 1000 + seq` (sequencial por entidade).
- **Business key:** `org_cd INT` (= `id` na fonte).
- **SCD2 markers:** `key_start_date`, `key_end_date` (`NULL` indica versão ativa).
- **Atributos versionados:** `name`, `short_name`, `description`, `is_active`.
- **Atributos não versionados:** `registration_date` (data de criação na plataforma).

**Lógica de change detection:** comparação entre a versão ativa (`key_end_date IS NULL`) e a fonte por `compare_cols`; se houver diferença, MERGE INTO fecha a antiga (`UPDATE key_end_date = modified - 1s`) e insere a nova (`key_start_date = modified + 1s`).

#### `dim_user` — SCD Type 2

Construída por **join completo** (`full_outer`) de `silver.auth_user` com `silver.auth_userprofile`.

- **Surrogate key:** `user_key BIGINT = user_cd * 1000 + version`.
- **Business key:** `user_cd INT` (= `id` na fonte de `auth_user`).
- **Marker de ativo:** `key_end_date = '9999-12-31'` (note a diferença vs. `dim_organization` — ver §11).
- **Hash:** `user_hash STRING = xxhash64` sobre os atributos versionados (`username`, nome, email, `is_staff`, `is_superuser`, `is_active`, demografia, contactos).

**Tratamento especial de `last_login`:** é versionado como SCD1 (UPDATE in-place) e não dispara nova versão SCD2. Lógica:

```sql
WHEN MATCHED AND merge_action = 'UPSERT_NEW_OR_SCD1'
              AND target.user_hash = source.user_hash THEN
    UPDATE SET target.last_login = source.last_login, ...
```

**Otimização:** join `BROADCAST` da lista de `user_cd` alterados antes do scan da target — evita ler a `dim_user` inteira quando o delta é pequeno.

#### `dim_course_edition` — SCD Type 2

Construída a partir de `silver.course_overviews_courseoverview` com **lookup SCD2 a `dim_organization`**:

```sql
JOIN dim_organization oo
  ON  upper(co.org) = upper(oo.short_name)
  AND co.event_ts BETWEEN oo.key_start_date
                      AND coalesce(oo.key_end_date, '9999-12-31 23:59:59')
```

Isto garante que cada edição é ligada à versão correta da entidade no momento em que foi modificada.

- **Surrogate key:** `course_edition_key STRING = '<course_edition_cd>_<seq:003>'` (ex.: `course-v1:UMinho+UMinho001+2024_T1_001`).
- **Business key:** `course_edition_cd STRING` (Open edX `id`).
- **`edition`:** extraído do `course_edition_cd` via regex `([^+]+)(?:\+ccx@.*)?$` (apanha o segmento RUN e ignora sufixos CCX).
- **FK SCD2 para `dim_organization`:** `org_key`.

**Detecção de mudança:** comparação null-safe (`<=>`) sobre 35+ colunas de negócio.

### 7.3 Factos

#### `fact_course_enrollment_daily` — granularidade diária

**O facto principal do produto.** Tem **uma linha por (inscrição, dia)** desde a inscrição até ao fim da edição (ou até hoje, ou até à desinscrição), o que permite responder a perguntas como _"quantos alunos estavam inscritos no dia X?"_ sem qualquer SQL temporal complexo.

- **Grão:** `(course_enrollment_cd, day_key)`
- **Partição:** `days(day_key)`
- **FKs SCD2-aware:** `course_edition_key`, `user_key`, `org_key`.
- **Métricas:** `is_enrolled` (boolean por dia), `unenrollment_date`, `course_enrollment_start_date`.

**Construção (resumida):**

1. Le delta de `silver.student_courseenrollment` (estado atual) e `silver.student_courseenrollment_history` (último evento).
2. Resolve dimensões com **between SCD2** (`event_ts BETWEEN key_start_date AND coalesce(key_end_date, '9999-12-31')`).
3. Calcula `effective_start = GREATEST(course_enrollment_start_date, course_edition.start_date)` e `effective_end = LEAST(course_enrollment_end_date, course_edition.end_date, today)`.
4. Garante `effective_end ≥ effective_start` para não perder _late enrollments_ (ver fix histórico no comit `5fc6f69`).
5. Explode com `sequence(effective_start, effective_end, interval 1 day)` → uma linha por dia.
6. **Dedup** por `(course_enrollment_cd, day_key)` antes do `MERGE INTO` para evitar `MERGE_CARDINALITY_VIOLATION` quando há sobreposição de versões SCD2.

**Otimizações:** `spark.sql.shuffle.partitions=200`, AQE + skew join enabled, broadcast remoto eliminado (depende de AQE), Iceberg `rewrite_data_files` com `max-concurrent-file-group-rewrites=20` e `partial-progress.enabled=true`.

#### `fact_certificate_daily`

- **Grão:** `(certificate_cd, day_key)` onde `day_key = DATE(certificate_issue_date)`.
- **FKs SCD2-aware:** `course_edition_key`, `user_key`, `org_key`.
- **Status:** `downloadable`, `notpassing`, `audit_passing`, `unverified`, etc. Para conclusão efetiva considera-se `status = 'downloadable'`.
- Resolve dimensões com `event_ts = GREATEST(modified_date, created_date)`.

#### `fact_course_edition_daily`

- **Grão:** `(course_edition_key, day_key)` para todos os dias em que a edição esteve "ativa" (entre `start_date` e `end_date` ou hoje).
- **Partição:** `day_key` (sem `days()` — é o próprio `DATE`).
- **Estratégia:** _"DELETE-then-append"_ — para cada `course_edition_key` afetado pelo delta, apaga linhas antigas e regenera.
- Faz join string-based contra `dim_time` (yyyyMMdd) para gerar o range diário.

#### `fact_student_grades`

- **Grão:** `(student_grade_key)` onde `student_grade_key = grades_persistentcoursegrade.id` (1:1 com a fonte).
- **FKs SCD2-aware (active row only):** `user_key` (de `dim_user` onde `key_end_date = '9999-12-31'`), `course_edition_key` (de `dim_course_edition` onde `key_end_date IS NULL`).
- **MERGE condicional:** `WHEN MATCHED AND src.modified > tgt.modified THEN UPDATE` — evita reverter a registos mais antigos.

### 7.4 Padrões comuns das tabelas gold

Todas as tabelas gold partilham:

| Aspeto | Detalhe |
|---|---|
| Storage | Iceberg + Parquet `zstd` |
| Tamanho-alvo de ficheiro | 128 MB (dim) / 512 MB (fact) |
| `write.distribution-mode` | `none` (dim) ou `hash` (fact em maior volume) |
| `write.sort.order` | Definido por tabela; ex.: `day_key ASC, course_edition_key ASC` |
| Bloom filters | Em colunas de FK e colunas de filtro frequente |
| Manutenção | `rewrite_data_files (sort)` + `rewrite_manifests` + `expire_snapshots(retain_last=5)` ao final de cada run |
| Audit metadata | `last_update_timestamp` em todas as tabelas |

---

## 8. Camada Gold/Reporting — Agregadas para Superset

Ficheiro: [`src/gold/gold_reporting_agg_tables.py`](../src/gold/gold_reporting_agg_tables.py).

Este script orquestra a construção de **6 datasets agregados** que servem diretamente os dashboards de Superset, evitando computações pesadas em runtime de visualização.

### 8.1 Datasets

| Tabela | Estratégia | Grão | KPIs principais |
|---|---|---|---|
| `fact_conclusion_rate_agg` | Full refresh | `(course_cd, edition, org_cd)` | Taxa de conclusão por curso/edição (`total_certificates / net_enrolled`) |
| `tickets_vs_courses_agg` | Incremental por partição `day_key` | `(day_key, course_cd, edition, org_cd)` | Cruzamento com `gold.gestao.jira_tickets` |
| `certificates_agg` | Incremental por partição `day_key` | `(day_key, course_cd, edition, org_cd)` | Volume diário de certificados |
| `enrollments_vs_certificates_agg` | Incremental por partição `day_key` | `(day_key, course_cd, edition, org_cd, status)` | Total de dias até conclusão |
| `enrollment_flow_agg` | Incremental + pushdown_day_filter | `(day_key, course_cd, edition, org_cd)` | `new_enrollments`, `new_unenrollments`, `active_students` (stock) |
| `enrollment_users_agg` | Full refresh | `(org_cd, course_cd, edition, user_key)` | Formandos únicos por edição (suporta filtros _is_currently_enrolled_) |

### 8.2 Estratégia de execução

```python
@dataclass
class AggTable:
    name: str
    sql_fn: Callable
    partition_by: str
    sort_order: str
    output_partitions: int = 50
    full_refresh: bool = False
    pushdown_day_filter: bool = False
```

Pontos relevantes implementados (conforme comentários FIX 1–9 no código):

- **Caching das fact tables hot** (`fact_course_enrollment_daily`, `fact_certificate_daily`) com `CACHE LAZY TABLE` — controlado por `CACHE_FACT_TABLES=true|false`.
- **Repartition explícito** antes do write para controlar contagem de ficheiros em S3.
- **Pre-aggregation por `(course_cd, edition, org_cd)`** em chaves naturais (não SCD2 surrogates) para evitar duplicação quando há múltiplas versões SCD2 da mesma edição.
- **Eliminação de `COUNT(DISTINCT)` empilhados** via two-stage aggregation (user_summary → enrollment_agg) — `MAX(CASE WHEN ... THEN 1 ELSE 0 END)` por user e depois `SUM(...)`.
- **Concorrência configurável** via `AGG_MAX_WORKERS` (default `1` para evitar OOM em backfill).
- **Filtragem opcional** via `TABLES_TO_RUN="t1,t2"` para reprocessar subconjuntos.
- **Manutenção Iceberg saltada na primeira run** — os ficheiros já estão otimamente dimensionados pelo `repartition()`.

### 8.3 Fluxo de incremental por partição

Para tabelas com `day_key`:

1. `_changed_days` = união dos dias com `last_update_timestamp > last_exec` em `fact_course_enrollment_daily ∪ fact_certificate_daily`.
2. SQL agregada é executada com filtro `INNER JOIN _changed_days` (ou pushdown direto na fact em `enrollment_flow_agg`).
3. `writeTo(...).overwritePartitions()` — apenas as partições afetadas são reescritas.

Para tabelas com `full_refresh=True` (sem `day_key`): `TRUNCATE TABLE` + `append`.

---

## 9. Operação, Deploy e CI/CD

### 9.1 Imagem Docker

O `Dockerfile` é minimalista — herda da imagem base `nauedu/nau-analytics-base-spark:latest` e instala a biblioteca partilhada:

```dockerfile
FROM nauedu/nau-analytics-base-spark:latest
USER root
RUN pip install --no-cache-dir git+https://github.com/fccn/nau-analytics-utils.git@main#subdirectory=common_libs/utils
USER 185
COPY src/ /opt/spark/work-dir/src/
WORKDIR /opt/spark/work-dir
USER 185
```

UID `185` corresponde ao utilizador `spark` na imagem base, requisito do Spark on Kubernetes Operator.

### 9.2 Pipeline GitHub Actions

Ficheiro: [`.github/workflows/docker-build-push.yml`](../.github/workflows/docker-build-push.yml).

Trigger: **push em qualquer branch** que altere `src/**` ou `Docker/**`. Comportamento:

| Branch | Tag pushada para `nauedu/nau-analytics-external-data-product` |
|---|---|
| `main` | `latest` + `<sha:7>` |
| Outras branches | `<branch-name>` (com `/` substituído por `-`) |

PRs constroem mas **não fazem push** — útil para validação.

### 9.3 Orquestração

Os DAGs Airflow vivem **noutro repositório** (Kubernetes deployment & infra). Cada script `*.py` é executado como **SparkApplication** via Spark Operator on Kubernetes, recebendo as variáveis de ambiente listadas em §10.

### 9.4 Sequência de execução típica

```
Bronze ─┬─ auth_user                         ─┐
        ├─ auth_userprofile                  ─┤
        ├─ certificates_generatedcertificate ─┤
        ├─ course_overviews_courseoverview   ─├──▶ Silver (1:1) ──▶ Gold ─┬─ dim_organization
        ├─ grades_persistentcoursegrade      ─┤                            ├─ dim_user
        ├─ organizations_organization        ─┤                            ├─ dim_course_edition (depende de dim_organization)
        ├─ organizations_historicalorganization (one-shot)                 ├─ dim_time
        ├─ student_courseaccessrole          ─┤                            │
        ├─ student_courseenrollment          ─┤                            ├─ fact_course_edition_daily (depende de dim_*)
        ├─ student_courseenrollment_history  ─┤                            ├─ fact_course_enrollment_daily
        └─ student_userattribute             ─┘                            ├─ fact_certificate_daily
                                                                           ├─ fact_student_grades
                                                                           │
                                                                           └─ gold_reporting_agg_tables
                                                                                  ▼
                                                                              Superset
```

Cada dimensão SCD2 depende das silver respetivas. Os factos dependem das **três dimensões SCD2 já materializadas** para resolver corretamente as `_key`.

---

## 10. Variáveis de Ambiente

Variáveis **obrigatórias** (lidas via `get_required_env`):

| Variável | Camada | Descrição |
|---|---|---|
| `ENVIRONMENT` | todas | Sufixo do catálogo: `_dev`, `_prod`, `_local`, ... |
| `MYSQL_HOST` | bronze | Host do MySQL Open edX |
| `MYSQL_PORT` | bronze | Porto do MySQL |
| `MYSQL_DATABASE` | bronze | DB MySQL (geralmente `edxapp`) |
| `MYSQL_USER` | bronze | User JDBC |
| `MYSQL_SECRET` | bronze | Password JDBC (idealmente injetada via K8s secret) |

Variáveis usadas apenas em `misc/get_full_tables.py` e `misc/incremental_load.py` (entrega manual da configuração Iceberg explícita):

| Variável | Descrição |
|---|---|
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_ENDPOINT` | Credenciais e URL S3 |
| `ICEBERG_CATALOG_HOST` / `ICEBERG_CATALOG_PORT` / `ICEBERG_DATABASE_CATALOG_NAME` | MySQL backing do catálogo Iceberg |
| `ICEBERG_CATALOG_USER` / `ICEBERG_CATALOG_PASSWORD` | Credenciais do catálogo (password é base64-decoded) |
| `ICEBERG_CATALOG_WAREHOUSE` | URI raiz do warehouse no S3 |
| `ICEBERG_CATALOG_NAME` | Nome lógico do catálogo |

Variáveis **opcionais** em `gold_reporting_agg_tables.py`:

| Variável | Default | Efeito |
|---|---|---|
| `TABLES_TO_RUN` | _(todas)_ | Lista CSV de agregadas a executar |
| `AGG_MAX_WORKERS` | `1` | Concorrência de execução paralela das agregadas |
| `CACHE_FACT_TABLES` | `true` | Cacheia `fact_course_enrollment_daily` e `fact_certificate_daily` em memória |

---

## 11. Observações e Pontos de Atenção

Esta secção lista observações relevantes detetadas durante a auditoria do código — úteis para evolução e onboarding de novos contribuidores.

### 11.1 Inconsistência no marker de SCD2 ativo

- `dim_user` usa `key_end_date = '9999-12-31 00:00:00'` para versões ativas.
- `dim_organization` e `dim_course_edition` usam `key_end_date IS NULL`.

Isto **propaga-se aos factos**: por exemplo, `fact_certificate_daily` e `fact_course_enrollment_daily` usam `coalesce(dce.key_end_date, '9999-12-31')` no `BETWEEN`, enquanto `fact_student_grades` filtra explicitamente `key_end_date = '9999-12-31'` para `dim_user` e `key_end_date IS NULL` para `dim_course_edition`. Funciona, mas é uma inconsistência conceptual — recomenda-se alinhar todas as dimensões para o mesmo padrão.

### 11.2 `validate_table_that_delete_lines` é informativo

Apesar do nome sugestivo, a função **nunca aborta** o pipeline em caso de divergência (o `raise Exception(...)` está comentado e o `return True` é fixo). Atualmente serve apenas como _placeholder_ para uma futura validação. Caso seja crítico detetar `DELETE`s na fonte, considerar:

1. Reativar a exceção, OU
2. Implementar tombstones em silver/gold com `is_deleted BOOLEAN`.

### 11.3 `validate_ingestion_values` tem o `raise` comentado

No mesmo espírito, a comparação de contagens entre src e tgt está desativada por código comentado. Recomenda-se reativar (ou converter em alerta no log) para detetar problemas silenciosos de ingestão.

### 11.4 Hardcoded lookups SCD2 na `dim_course_edition`

O join `upper(co.org) = upper(oo.short_name)` para resolver `org_key` depende de `short_name` ser único e estável. Se uma entidade alterar `short_name`, a versão SCD2 antiga deixa de _matchar_ e a edição passa a ter `org_key = NULL`. Considerar usar a chave natural já estável (`org_cd`) se disponível na fonte, ou validação periódica.

### 11.5 `dim_time` tem feriados mínimos

Apenas `New Year` e `Christmas` estão na lista de feriados (e ainda assim apenas em 1900). Para uso real em Portugal continental/regiões, recomenda-se popular dinamicamente via biblioteca tipo `holidays-pt` ou tabela externa. _Atualmente o flag `is_holiday` será sempre `false` na prática._

### 11.6 `incremental_load.py` em `bronze/python/misc/` é legacy

Este script aparenta ser de um padrão anterior (CSV de metadata em S3), não está integrado no fluxo regular e duplica funções já presentes em `bronze_utils_functions.py`. Avaliar se é seguro arquivar/remover.

### 11.7 Diretórios vazios

Os diretórios `src/misc/` e `src/shared/` estão vazios. Provavelmente reservados para evolução futura — considerar remover se não houver plano concreto.

### 11.8 Compilados Python comitados

`src/gold/__pycache__/` está commitado. Adicionar `__pycache__/` ao `.gitignore`.

---

## Apêndice A — Mapeamento Coluna a Coluna (origem → silver)

### A.1 `auth_user`

| Origem MySQL | Silver | Tipo | Notas |
|---|---|---|---|
| `id` | `id` | INT | PK |
| `last_login` | `last_login` | TIMESTAMP | |
| `is_superuser` | `is_superuser` | BOOLEAN | |
| `username` | `username` | STRING | |
| `first_name` | `first_name` | STRING | |
| `last_name` | `last_name` | STRING | |
| `email` | `email` | STRING | |
| `is_staff` | `is_staff` | BOOLEAN | |
| `is_active` | `is_active` | BOOLEAN | |
| `date_joined` | `date_joined` | TIMESTAMP | |
| _(calculado)_ | `row_hash` | STRING | SHA1 dos atributos versionáveis |
| _(metadata)_ | `ingestion_date` | TIMESTAMP | Marca de água |

### A.2 `auth_userprofile`

A coluna `meta` (JSON) contém atributos extra. Em silver é extraído `employment_situation` via `get_json_object(meta, '$.employment_situation')`. Os campos `courseware`, `mailing_address`, `goals`, `bio`, `profile_image_uploaded_at` são removidos por não terem uso analítico atual.

### A.3 `course_overviews_courseoverview`

Campos especiais:

- `start` é `GREATEST(start, '2015-01-01')` — proteção contra datas inválidas.
- URLs de imagem (`course_image_url`, `social_sharing_url`, `banner_image_url`, `marketing_url`, `course_video_url`) são removidos em silver.
- `_pre_requisite_courses_json` removido em silver.

---

## Apêndice B — Glossário

| Termo | Significado |
|---|---|
| **Open edX** | Plataforma LMS open-source utilizada pela NAU como sistema de origem |
| **Iceberg** | Formato de tabela transacional (Apache) sobre Parquet — suporta MERGE, time travel e schema evolution |
| **SCD2** | Slowly Changing Dimension Type 2 — versionamento de dimensões via `key_start_date`/`key_end_date` |
| **Medallion** | Padrão Bronze/Silver/Gold para data lakes (Databricks) |
| **CDC** | Change Data Capture — neste repositório implementado via hash de colunas |
| **Surrogate key** | Chave artificial (`_key`) gerada pelo data product, distinta da chave natural da fonte (`_cd`) |
| **NAU** | Plataforma nacional de cursos online da FCCN, baseada em Open edX |
| **FCCN** | Unidade da FCT que opera a infraestrutura |

---

_Documento gerado a partir do código atual (branch `feature/ingestion-script-improvements`, último commit `5fc6f69`)._
