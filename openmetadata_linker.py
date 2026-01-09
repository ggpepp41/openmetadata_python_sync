import argparse
import ast
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


TAG_PATTERN = re.compile(r"openmetadata:(upstream|downstream)\(([^:()]+):([^)]+)\)")
SECTION_UNDERLINE = re.compile(r"^[=~`^#*\-]{3,}\s*$")


@dataclass
class MetadataRef:
    direction: str  # 'upstream' or 'downstream'
    application: str
    field: str


@dataclass
class FunctionEntry:
    name: str
    lineno: int
    docstring: Optional[str]
    tags: List[MetadataRef] = field(default_factory=list)


@dataclass
class FileEntry:
    path: Path
    functions: List[FunctionEntry]


def _split_app_field(text: str) -> Optional[Tuple[str, str]]:
    text = text.strip()
    if not text:
        return None
    idx = text.find(":")
    if idx == -1:
        return None
    return text[:idx].strip(), text[idx + 1 :].strip()


def _extract_openmetadata_section_lines(docstring: str) -> List[str]:
    lines = docstring.splitlines()
    n = len(lines)
    # Pattern A: reST section heading
    for i in range(n - 1):
        if lines[i].strip().lower() == "openmetadata" and SECTION_UNDERLINE.match(lines[i + 1]):
            # Collect until next heading or end
            content: List[str] = []
            j = i + 2
            while j < n:
                # Stop on next heading (non-empty line followed by underline)
                if j + 1 < n and lines[j].strip() and SECTION_UNDERLINE.match(lines[j + 1]):
                    break
                content.append(lines[j])
                j += 1
            return content
    # Pattern B: label-style section: "OpenMetadata:" then indented block until blank line
    for i in range(n):
        if lines[i].strip().lower().startswith("openmetadata:"):
            after = lines[i].split(":", 1)[1]
            content = [after] if after.strip() else []
            j = i + 1
            while j < n and lines[j].strip():
                content.append(lines[j])
                j += 1
            return content
    return []


def parse_docstring_for_tags(docstring: Optional[str]) -> List[MetadataRef]:
    if not docstring:
        return []
    refs: List[MetadataRef] = []

    # 1) Sphinx/reST section parsing
    section_lines = _extract_openmetadata_section_lines(docstring)
    for raw in section_lines:
        line = raw.strip()
        if not line:
            continue
        # Bullet or plain directive: upstream|downstream: app:field[, app:field]
        m = re.match(r"^(?:-\s*)?(upstream|downstream)\s*:\s*(.+)$", line, re.IGNORECASE)
        if m:
            direction = m.group(1).lower()
            rest = m.group(2)
            for part in rest.split(','):
                parsed = _split_app_field(part)
                if parsed:
                    app, fld = parsed
                    refs.append(MetadataRef(direction=direction, application=app, field=fld))

    # 2) Sphinx field list style: :openmetadata-upstream: app:field[, app:field]
    for raw in docstring.splitlines():
        line = raw.strip()
        m = re.match(r"^:openmetadata-(upstream|downstream):\s*(.+)$", line, re.IGNORECASE)
        if m:
            direction = m.group(1).lower()
            rest = m.group(2)
            for part in rest.split(','):
                parsed = _split_app_field(part)
                if parsed:
                    app, fld = parsed
                    refs.append(MetadataRef(direction=direction, application=app, field=fld))

    # 3) Backward-compatible inline tags
    for match in TAG_PATTERN.finditer(docstring):
        direction, application, field = match.groups()
        refs.append(MetadataRef(direction=direction, application=application.strip(), field=field.strip()))

    # Deduplicate
    unique: Dict[Tuple[str, str, str], MetadataRef] = {}
    for r in refs:
        key = (r.direction, r.application, r.field)
        unique[key] = r
    return list(unique.values())


def extract_functions_from_file(file_path: Path) -> FileEntry:
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions: List[FunctionEntry] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node)
            tags = parse_docstring_for_tags(doc)
            functions.append(FunctionEntry(name=node.name, lineno=node.lineno, docstring=doc, tags=tags))
    return FileEntry(path=file_path, functions=functions)


def scan_python_files(root: Path) -> List[FileEntry]:
    entries: List[FileEntry] = []
    for path in root.rglob("*.py"):
        if path.name.startswith("__"):
            continue
        try:
            entries.append(extract_functions_from_file(path))
        except SyntaxError as e:
            print(f"Skipping {path} due to SyntaxError: {e}", file=sys.stderr)
        except UnicodeDecodeError as e:
            print(f"Skipping {path} due to decode error: {e}", file=sys.stderr)
    return entries


def summarize(entries: List[FileEntry]) -> List[Dict]:
    out: List[Dict] = []
    for fe in entries:
        funcs = []
        for fn in fe.functions:
            funcs.append({
                "name": fn.name,
                "lineno": fn.lineno,
                "docstring": fn.docstring,
                "tags": [
                    {
                        "direction": t.direction,
                        "application": t.application,
                        "field": t.field,
                    }
                    for t in fn.tags
                ],
            })
        out.append({
            "file": str(fe.path),
            "functions": funcs,
        })
    return out


def load_config(config_path: Optional[Path]) -> Dict:
    if not config_path:
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        import yaml  # type: ignore
        return yaml.safe_load(f) or {}


def ensure_openmetadata_client(config: Dict):
    try:
        from scripts.om_client import OpenMetadataHelper
    except Exception as e:
        raise RuntimeError(
            "OpenMetadata integration not available. Install dependencies and provide config."
        ) from e
    return OpenMetadataHelper.from_config(config)


def create_openmetadata_entries(entries: List[FileEntry], config: Dict, dry_run: bool = True):
    """
    Create OpenMetadata entries for each file (as Pipeline) and function (as Task),
    and connect upstream/downstream lineage to mapped application:field entities.
    """
    if dry_run:
        print("[DRY-RUN] Skipping OpenMetadata API calls. Parsed tags shown below.")
        print(json.dumps(summarize(entries), indent=2))
        return

    client = ensure_openmetadata_client(config)

    for fe in entries:
        # Create or get Pipeline for the file
        pipeline = client.ensure_pipeline_for_file(fe.path)

        for fn in fe.functions:
            task = client.ensure_task_for_function(pipeline, fe.path, fn.name, fn.lineno)
            # Connect lineage
            for tag in fn.tags:
                ref = client.resolve_application_field(tag.application, tag.field)
                if not ref:
                    print(f"Warning: No mapping found for {tag.application}:{tag.field}")
                    continue
                if tag.direction == "upstream":
                    client.create_lineage(from_ref=ref, to_task=task)
                else:
                    client.create_lineage(from_task=task, to_ref=ref)


def main():
    parser = argparse.ArgumentParser(description="Parse Python docstrings for OpenMetadata tags and create lineage")
    parser.add_argument("root", type=str, help="Root directory to scan for .py files")
    parser.add_argument("--config", type=str, help="Path to YAML config for OpenMetadata integration", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Do not call OpenMetadata; print parsed output")
    parser.add_argument("--output", type=str, help="Optional path to write JSON summary", default=None)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        print(f"Root path does not exist: {root}", file=sys.stderr)
        sys.exit(1)

    entries = scan_python_files(root)

    config = load_config(Path(args.config)) if args.config else {}

    if args.output:
        summary = summarize(entries)
        Path(args.output).write_text(json.dumps(summary, indent=2), encoding="utf-8")

    create_openmetadata_entries(entries, config=config, dry_run=args.dry_run or not args.config)


if __name__ == "__main__":
    main()
