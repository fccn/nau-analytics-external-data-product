# `docs/` — Documentação do PD Entidades Externas

Pasta com toda a documentação técnica e funcional do data product.

## Ficheiros

| Ficheiro | Conteúdo |
|---|---|
| [`PD_Entidades_Externas.md`](PD_Entidades_Externas.md) | **Documentação master** em Markdown — fonte da verdade, fácil de manter via PR |
| [`PD_Entidades_Externas.confluence.xml`](PD_Entidades_Externas.confluence.xml) | Mesmo conteúdo em **Confluence Storage Format** (XHTML) — pronto a colar no editor de fonte |
| [`diagrams/01_medallion_architecture.drawio`](diagrams/01_medallion_architecture.drawio) | Arquitetura medallion (origem → bronze → silver → gold → Superset) |
| [`diagrams/02_gold_layer_erd.drawio`](diagrams/02_gold_layer_erd.drawio) | ERD do gold layer (star schema) |
| [`diagrams/03_bronze_ingestion_patterns.drawio`](diagrams/03_bronze_ingestion_patterns.drawio) | Os 3 padrões de ingestão da camada bronze |

## Como publicar na página Confluence

A página de destino é [PD - Entidades Externas](https://glinttdev.atlassian.net/wiki/spaces/FIDDP/pages/2080632274947/PD+-+Entidades+Externas) (espaço **FIDDP**).

### Opção 1 — Storage format (rápido, fica idêntico ao master)

1. **Anexar os diagramas** à página Confluence:
   - Abrir a página → menu `...` → **Attachments** → adicionar os 3 ficheiros `docs/diagrams/*.drawio`.
   - Confirmar que o nome dos anexos é exatamente:
     - `01_medallion_architecture`
     - `02_gold_layer_erd`
     - `03_bronze_ingestion_patterns`
   - (Sem extensão `.drawio` no atributo `diagramName` da macro.)
2. **Editar a página** → menu `...` → **View source** (atalho `Ctrl+Shift+S` ou `< >`).
3. Abrir [`PD_Entidades_Externas.confluence.xml`](PD_Entidades_Externas.confluence.xml), copiar o conteúdo **entre** os comentários `<!-- ROOT_START -->` e `<!-- ROOT_END -->` e colar no editor de fonte.
4. Guardar a página. Os diagramas drawio renderizam automaticamente (a app drawio.com tem de estar instalada na instância — está, é o caso da `glinttdev.atlassian.net`).

### Opção 2 — Render a partir do Markdown

Caso prefira manter o master apenas em Markdown e renderizar com plugins:

- Macro `markdown-from-url` ou `include-page` apontando para o ficheiro raw no GitHub.
- Os diagramas drawio podem ser embebidos via macro `drawio` apontando para os anexos da página (ou para o repositório git, se o plugin permitir).

## Manter sincronizado

Sempre que houver alterações ao código:

1. Atualizar o master em `PD_Entidades_Externas.md` (PR + review).
2. Regenerar/manter consistente o `PD_Entidades_Externas.confluence.xml`.
3. Atualizar diagramas em `diagrams/*.drawio` (abrir em <https://app.diagrams.net> ou no plugin VS Code da drawio).
4. Republicar na página Confluence (passos da Opção 1).
