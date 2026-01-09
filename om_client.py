from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class EntityRef:
    type: str  # e.g., 'table', 'dashboard', 'pipeline'
    fqn: str
    id: Optional[str] = None  # if resolved from server


class OpenMetadataHelper:
    """Lightweight helper to either emit requests (offline) or call OpenMetadata (online)."""

    def __init__(self, config: Dict):
        self.config = config or {}
        self.requests_path = Path(self.config.get("requestsPath", "openmetadata_requests.json")).resolve()
        self._requests: Dict = {"services": [], "pipelines": [], "lineage": []}
        self.offline = True
        try:
            # Try to import OpenMetadata client; if fails, remain offline
            from metadata.ingestion.ometa.ometa_api import OpenMetadata  # noqa: F401
            self.offline = False
        except Exception:
            self.offline = True

        if not self.offline:
            # Initialize OpenMetadata client if possible
            self._init_client()

    @classmethod
    def from_config(cls, config: Dict) -> "OpenMetadataHelper":
        return cls(config)

    def _init_client(self):
        # Lazy import to avoid hard dependency if not needed
        from metadata.ingestion.ometa.ometa_api import OpenMetadata
        from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
            OpenMetadataJWTClientConfig,
        )
        from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
            OpenMetadataConnection,
        )

        om_cfg = self.config.get("openmetadata", {})
        host_port = om_cfg.get("hostPort")
        jwt_token = om_cfg.get("jwtToken")
        if not host_port or not jwt_token:
            raise RuntimeError("Missing openmetadata.hostPort or openmetadata.jwtToken in config")

        client_config = OpenMetadataJWTClientConfig(jwtToken=jwt_token)
        connection = OpenMetadataConnection(hostPort=host_port, authProvider="openmetadata", securityConfig=client_config)
        self._client = OpenMetadata(connection)

    def _flush_offline(self):
        self.requests_path.write_text(json.dumps(self._requests, indent=2), encoding="utf-8")

    def ensure_pipeline_for_file(self, file_path: Path) -> EntityRef:
        name = self._pipeline_name_for_file(file_path)
        service = self.config.get("pipelineServiceName", "code-service")
        fqn = f"{service}.{name}"
        ref = EntityRef(type="pipeline", fqn=fqn)
        if self.offline:
            self._requests["pipelines"].append({
                "action": "ensure",
                "service": service,
                "name": name,
                "file": str(file_path),
            })
            self._flush_offline()
            return ref

        # Online mode: ensure service and pipeline
        from metadata.generated.schema.entity.services.pipelineService import CreatePipelineServiceRequest
        from metadata.generated.schema.entity.services.serviceType import ServiceType
        from metadata.generated.schema.entity.data.pipeline import CreatePipelineRequest

        # Ensure service exists
        service_req = CreatePipelineServiceRequest(name=service, serviceType=ServiceType.Pipeline)
        try:
            self._client.create_or_update(service_req)
        except Exception:
            pass

        # Ensure pipeline
        pipeline_req = CreatePipelineRequest(name=name, service=service)
        pipeline = self._client.create_or_update(pipeline_req)
        return EntityRef(type="pipeline", fqn=pipeline.fullyQualifiedName.__root__, id=str(pipeline.id.__root__))

    def ensure_task_for_function(self, pipeline: EntityRef, file_path: Path, func_name: str, lineno: int) -> EntityRef:
        # In offline mode, we just record the task creation intent. In online mode, we update pipeline tasks.
        task_fqn = f"{pipeline.fqn}.{func_name}"
        ref = EntityRef(type="pipelineTask", fqn=task_fqn)
        if self.offline:
            self._requests["pipelines"].append({
                "action": "ensureTask",
                "pipeline": pipeline.fqn,
                "task": func_name,
                "file": str(file_path),
                "lineno": lineno,
            })
            self._flush_offline()
            return ref

        # Online: update pipeline tasks
        from metadata.generated.schema.entity.data.pipeline import Pipeline
        current = self._get_entity_by_name("pipeline", pipeline.fqn)
        if current and isinstance(current, Pipeline):
            tasks = current.tasks or []
            names = {t.name for t in tasks}
            if func_name not in names:
                from metadata.generated.schema.entity.data.pipeline import PipelineTask
                tasks.append(PipelineTask(name=func_name, fullyQualifiedName=task_fqn))
                current.tasks = tasks
                self._client.update_entity(current)
            return EntityRef(type="pipelineTask", fqn=task_fqn, id=None)
        return ref

    def resolve_application_field(self, application: str, field: str) -> Optional[EntityRef]:
        apps = self.config.get("applications", {})
        app = apps.get(application)
        if not app:
            return None
        # Column-level mapping if available
        columns = app.get("columns", {})
        col_fqn = columns.get(field)
        if col_fqn:
            return EntityRef(type="column", fqn=col_fqn)
        # Fallback to entity-level
        ent_type = app.get("type", "table")
        ent_fqn = app.get("fqn")
        if not ent_fqn:
            return None
        return EntityRef(type=ent_type, fqn=ent_fqn)

    def create_lineage(self, from_ref: Optional[EntityRef] = None, to_task: Optional[EntityRef] = None, from_task: Optional[EntityRef] = None, to_ref: Optional[EntityRef] = None):
        if self.offline:
            self._requests["lineage"].append({
                "from": from_ref.fqn if from_ref else (from_task.fqn if from_task else None),
                "fromType": from_ref.type if from_ref else (from_task.type if from_task else None),
                "to": to_ref.fqn if to_ref else (to_task.fqn if to_task else None),
                "toType": to_ref.type if to_ref else (to_task.type if to_task else None),
            })
            self._flush_offline()
            return

        # Online: add lineage between entities (using entity references). PipelineTask lineage may not be supported; fallback to pipeline
        from_ref_use = from_ref
        to_ref_use = to_ref
        if (from_task and not from_ref_use) or (to_task and not to_ref_use):
            # Resolve pipeline entity from task fqn prefix
            def pipeline_fqn(task_fqn: str) -> str:
                return task_fqn.rsplit(".", 1)[0]
            if from_task and not from_ref_use:
                from_ref_use = self._entity_ref_from_fqn("pipeline", pipeline_fqn(from_task.fqn))
            if to_task and not to_ref_use:
                to_ref_use = self._entity_ref_from_fqn("pipeline", pipeline_fqn(to_task.fqn))

        if not from_ref_use or not to_ref_use:
            return

        self._add_lineage_edge(from_ref_use, to_ref_use)

    # ---- Internal helpers (online mode) ----
    def _get_entity_by_name(self, entity_type: str, fqn: str):
        try:
            return self._client.get_by_name(entity_type, fqn)
        except Exception:
            return None

    def _entity_ref_from_fqn(self, entity_type: str, fqn: str) -> Optional[EntityRef]:
        ent = self._get_entity_by_name(entity_type, fqn)
        if not ent:
            return None
        try:
            eid = str(ent.id.__root__)
        except Exception:
            eid = None
        return EntityRef(type=entity_type, fqn=fqn, id=eid)

    def _add_lineage_edge(self, from_ref: EntityRef, to_ref: EntityRef):
        from metadata.generated.schema.type.entityReference import EntityReference
        from metadata.generated.schema.type.structuredLineage import AddLineageRequest, EntitiesEdge

        edge = EntitiesEdge(
            fromEntity=EntityReference(id=from_ref.id, type=from_ref.type),
            toEntity=EntityReference(id=to_ref.id, type=to_ref.type),
        )
        req = AddLineageRequest(edge=edge)
        self._client.add_lineage(req)

    @staticmethod
    def _pipeline_name_for_file(file_path: Path) -> str:
        # Create a stable name from path, replacing separators with underscores
        return (
            str(file_path.relative_to(Path.cwd())).replace("\\", "_").replace("/", "_")
        )
