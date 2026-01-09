OpenMetadata Linker

This tool scans Python files for function docstrings and looks for an OpenMetadata section using Sphinx/reStructuredText style. It supports:

- Section heading:

  OpenMetadata
  ------------

  - upstream: myapp:fieldA, otherapp:fieldB
  - downstream: myapp:fieldC

- Field list style:

  :openmetadata-upstream: myapp:fieldA, otherapp:fieldB
  :openmetadata-downstream: myapp:fieldC

Legacy inline tags are also supported:

- openmetadata:upstream(application:field)
- openmetadata:downstream(application:field)

The tool can optionally create OpenMetadata entries to model the file as a Pipeline with one Task per function, then connect lineage to the referenced application fields.

Usage

Dry run (parse and print):

```bash
python scripts/openmetadata_linker.py f --dry-run --output parsed_metadata.json
```

With OpenMetadata integration:

```bash
pip install pyyaml openmetadata-ingestion
python scripts/openmetadata_linker.py f --config scripts/openmetadata_config.example.yaml
```

Config schema (YAML)

```yaml
openmetadata:
  hostPort: "http://localhost:8585"
  jwtToken: "<your-jwt-token>"

pipelineServiceName: "code-service"

applications:
  myapp:
    type: table
    fqn: "dbService.database.schema.table"
    columns:
      fieldA: "dbService.database.schema.table.columnA"
      fieldB: "dbService.database.schema.table.columnB"
```

Notes

- If `openmetadata-ingestion` is not installed, the script will run in offline mode when a config is provided and write intended requests to `openmetadata_requests.json`.
- In online mode, it will attempt to ensure the pipeline service and pipelines exist, then add lineage edges between data assets and pipelines. Column-level lineage uses the `columns` mapping when available.
- In Sphinx/reST docstrings, use either an underlined "OpenMetadata" section or field list entries. Multiple references can be comma-separated.
