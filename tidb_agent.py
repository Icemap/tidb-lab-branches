import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from pytidb import TiDBClient
from requests.auth import HTTPDigestAuth
from strands import Agent, tool
from strands.models.bedrock import BedrockModel, DEFAULT_BEDROCK_MODEL_ID

# Load environment variables early so both the agent and Streamlit UI can use them.
load_dotenv()


def _env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def _build_tidb_client() -> TiDBClient:
    """Create a TiDB client from .env values."""
    host = _env("SERVERLESS_CLUSTER_HOST")
    port = int(os.getenv("SERVERLESS_CLUSTER_PORT", "4000"))
    username = _env("SERVERLESS_CLUSTER_USERNAME")
    password = _env("SERVERLESS_CLUSTER_PASSWORD")
    database = _env("SERVERLESS_CLUSTER_DATABASE_NAME")
    enable_ssl = host.endswith("tidbcloud.com")
    return TiDBClient.connect(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        enable_ssl=enable_ssl,
    )


@dataclass
class BranchManager:
    """Minimal wrapper for TiDB Cloud branch API."""

    public_key: Optional[str] = None
    private_key: Optional[str] = None
    access_token: Optional[str] = None
    cluster_id: Optional[str] = None
    project_id: Optional[str] = None
    base_url: str = "https://serverless.tidbapi.com/v1beta1"
    admin_base_url: str = "https://api.tidbcloud.com"

    @property
    def auth(self) -> HTTPDigestAuth:
        if not self.public_key or not self.private_key:
            raise RuntimeError(
                "Provide TIDBCLOUD_ACCESS_TOKEN or both TIDBCLOUD_PUBLIC_KEY and TIDBCLOUD_PRIVATE_KEY."
            )
        return HTTPDigestAuth(self.public_key, self.private_key)

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Send an HTTP request using the configured auth mechanism."""
        if path.startswith("http"):
            url = path
        else:
            url = f"{self.base_url}{path}"
        headers: Dict[str, str] = dict(kwargs.pop("headers", {}) or {})
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"
        else:
            kwargs["auth"] = self.auth
        if headers:
            kwargs["headers"] = headers
        kwargs.setdefault("timeout", 30)
        return requests.request(method, url, **kwargs)

    def _get_default_cluster(self) -> str:
        """Fetch the first cluster id when none is provided."""
        resp = self._request("GET", "/clusters")
        resp.raise_for_status()
        data = resp.json()
        clusters = data.get("clusters") or []
        if not clusters:
            raise RuntimeError("No clusters found for the current API key.")
        # API returns clusterId field
        return clusters[0].get("clusterId") or clusters[0].get("id")

    def _cluster(self) -> str:
        if self.cluster_id:
            return self.cluster_id
        self.cluster_id = self._get_default_cluster()
        return self.cluster_id

    def list_branches(self) -> List[Dict[str, Any]]:
        resp = self._request("GET", f"/clusters/{self._cluster()}/branches")
        resp.raise_for_status()
        return resp.json().get("branches", [])

    def create_branch(
        self,
        display_name: str,
        parent_id: Optional[str] = None,
        parent_timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"displayName": display_name}
        if parent_id:
            payload["parentId"] = parent_id
        if parent_timestamp:
            payload["parentTimestamp"] = parent_timestamp
        resp = self._request(
            "POST",
            f"/clusters/{self._cluster()}/branches",
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def get_branch(self, branch_id: str) -> Dict[str, Any]:
        resp = self._request("GET", f"/clusters/{self._cluster()}/branches/{branch_id}")
        resp.raise_for_status()
        return resp.json()

    def delete_branch(self, branch_id: str) -> Dict[str, Any]:
        resp = self._request(
            "DELETE",
            f"/clusters/{self._cluster()}/branches/{branch_id}",
        )
        resp.raise_for_status()
        return resp.json()

    def reset_branch(self, branch_id: str) -> Dict[str, Any]:
        resp = self._request(
            "POST",
            f"/clusters/{self._cluster()}/branches/{branch_id}:reset",
            json={},
        )
        resp.raise_for_status()
        return resp.json()

    def create_backup(self, display_name: Optional[str] = None) -> Dict[str, Any]:
        """Create a lightweight branch backup before DB operations."""
        backup_name = display_name or f"backup-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        return self.create_branch(display_name=backup_name)

    def create_admin_user_for_branch(
        self,
        *,
        project_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
        branch_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Request TiDB Cloud to create an admin user for the given branch."""
        resolved_project = project_id or self.project_id
        resolved_cluster = cluster_id or self._cluster()
        if not resolved_project or not resolved_cluster or not branch_name:
            raise RuntimeError("Project ID, cluster ID, and branch_name are required to create branch admin users.")
        host_prefix = (
            self.admin_base_url.split("/v1beta1")[0]
            if "/v1beta1" in self.admin_base_url
            else self.admin_base_url.rstrip("/")
        )
        url = (
            f"{host_prefix}/api/internal/projects/{resolved_project}"
            f"/clusters/{resolved_cluster}/branches/{branch_name}/users"
        )
        resp = self._request("POST", url, json={"type": "Admin"})
        resp.raise_for_status()
        return resp.json()


# Global, intentionally simple demo-level instances (lazy to keep tests light).
_access_token = (os.getenv("TIDBCLOUD_ACCESS_TOKEN") or "").strip() or None
_public_key = os.getenv("TIDBCLOUD_PUBLIC_KEY")
_private_key = os.getenv("TIDBCLOUD_PRIVATE_KEY")
_project_id = os.getenv("TIDBCLOUD_PROJECT_ID")

_branch_manager: Optional[BranchManager] = None
_tidb_client: Optional[TiDBClient] = None

CREDENTIALS_FILE = Path(__file__).with_name("branch_credentials.csv")
DEFAULT_DATABASE = os.getenv("SERVERLESS_CLUSTER_DATABASE_NAME")
DEFAULT_PORT = int(os.getenv("SERVERLESS_CLUSTER_PORT", "4000"))
DEFAULT_HOST = os.getenv("SERVERLESS_CLUSTER_HOST")


def _load_admin_credentials() -> Dict[tuple[str, str], Dict[str, str]]:
    """Read cached admin creds from CSV."""
    if not CREDENTIALS_FILE.exists():
        return {}
    records: Dict[tuple[str, str], Dict[str, str]] = {}
    with CREDENTIALS_FILE.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row.get("branchName", ""), row.get("clusterId", ""))
            records[key] = {
                "branchName": row.get("branchName", ""),
                "clusterId": row.get("clusterId", ""),
                "username": row.get("username", ""),
                "password": row.get("password", ""),
                "host": row.get("host", ""),
                "port": int(row["port"]) if row.get("port") else None,
                "database": row.get("database", ""),
            }
    return records


def _write_admin_credentials(records: Dict[tuple[str, str], Dict[str, str]]) -> None:
    """Persist all admin credential entries to CSV."""
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CREDENTIALS_FILE.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["branchName", "clusterId", "username", "password", "host", "port", "database"]
        )
        writer.writeheader()
        for entry in records.values():
            writer.writerow(entry)


def _store_admin_credential(entry: Dict[str, Any]) -> None:
    """Update cache with a credential entry and persist to CSV."""
    cache = _load_admin_credentials()
    key = (entry.get("branchName", ""), entry.get("clusterId", ""))
    cache[key] = {
        "branchName": entry.get("branchName", ""),
        "clusterId": entry.get("clusterId", ""),
        "username": entry.get("username", ""),
        "password": entry.get("password", ""),
        "host": entry.get("host", ""),
        "port": entry.get("port"),
        "database": entry.get("database", ""),
    }
    _write_admin_credentials(cache)


def _get_cached_admin_credential(branch_name: str, cluster_id: str) -> Optional[Dict[str, str]]:
    cache = _load_admin_credentials()
    return cache.get((branch_name, cluster_id))


def _delete_admin_credential(branch_name: str, cluster_id: str) -> None:
    """Remove cached admin creds for a branch and persist."""
    cache = _load_admin_credentials()
    key = (branch_name, cluster_id)
    if key in cache:
        del cache[key]
        _write_admin_credentials(cache)


def _get_branch_manager() -> BranchManager:
    """Lazily create the branch manager to avoid requiring credentials at import time."""
    global _branch_manager
    if _branch_manager is None:
        if not _access_token and (_public_key is None or _private_key is None):
            raise RuntimeError(
                "Set TIDBCLOUD_ACCESS_TOKEN or both TIDBCLOUD_PUBLIC_KEY and TIDBCLOUD_PRIVATE_KEY in the .env file."
            )
        _branch_manager = BranchManager(
            public_key=_public_key,
            private_key=_private_key,
            access_token=_access_token,
            cluster_id=os.getenv("SERVERLESS_CLUSTER_ID"),
            project_id=_project_id,
        )
    return _branch_manager


def _get_tidb_client() -> TiDBClient:
    """Lazily create the default TiDB client."""
    global _tidb_client
    if _tidb_client is None:
        _tidb_client = _build_tidb_client()
    return _tidb_client


def _resolve_branch_id(display_name: str) -> Optional[str]:
    """Find a branch id by display name."""
    branches = _get_branch_manager().list_branches()
    for br in branches:
        if br.get("displayName") == display_name:
            return br.get("branchId")
    return None


def _wait_for_branch_active(manager: BranchManager, branch_id: str, *, retries: int = 20, delay_seconds: int = 10) -> None:
    """Poll branch state until ACTIVE or until retries exhausted."""
    for attempt in range(retries):
        info = manager.get_branch(branch_id)
        if info.get("state") == "ACTIVE":
            return
        time.sleep(delay_seconds)
    raise RuntimeError(f"Branch {branch_id} not active after {retries * delay_seconds} seconds.")


def _ensure_admin_credential(branch_name: str, cluster_id: Optional[str] = None) -> Dict[str, str]:
    """Fetch or create admin credentials for a branch, filling host/port/database from branch endpoints."""
    manager = _get_branch_manager()
    resolved_cluster = cluster_id or manager._cluster()
    if not _project_id:
        raise RuntimeError("TIDBCLOUD_PROJECT_ID is required to create branch admin users.")
    cached = _get_cached_admin_credential(branch_name, resolved_cluster)
    if cached and cached.get("host") and cached.get("port"):
        payload = {"source": "cache", **cached}
    else:
        branch_id = _resolve_branch_id(branch_name)
        if branch_id is None:
            raise RuntimeError(f"Branch `{branch_name}` not found; ensure it exists before requesting credentials.")
        _wait_for_branch_active(manager, branch_id)
        branch_info = manager.get_branch(branch_id)
        endpoints = (branch_info.get("endpoints") or {}).get("public") or {}
        admin_info = manager.create_admin_user_for_branch(
            branch_name=branch_name,
            cluster_id=resolved_cluster,
        )
        # Some APIs may only populate endpoints after admin creation; refresh if host/port missing.
        if not endpoints.get("host") or not endpoints.get("port"):
            branch_info = manager.get_branch(branch_id)
            endpoints = (branch_info.get("endpoints") or {}).get("public") or {}
        entry = {
            "branchName": branch_name,
            "clusterId": resolved_cluster,
            "username": admin_info.get("username", ""),
            "password": admin_info.get("password", ""),
            "host": endpoints.get("host"),
            "port": endpoints.get("port"),
            "database": DEFAULT_DATABASE or "",
        }
        payload = {"source": "created", **entry}
        _store_admin_credential(payload)

    # Host/port must come from branch endpoints to avoid main-cluster reuse; database still defaults.
    if not payload.get("host"):
        raise RuntimeError("Branch endpoints missing host; branch may not be ready or API response incomplete.")
    if not payload.get("port"):
        raise RuntimeError("Branch endpoints missing port; branch may not be ready or API response incomplete.")
    if DEFAULT_DATABASE and not payload.get("database"):
        payload["database"] = DEFAULT_DATABASE
    return payload


def _connect_branch_client(creds: Dict[str, str]) -> TiDBClient:
    """Create a TiDB client pointing at a specific branch host with provided admin creds."""
    host = creds.get("host")
    if not host:
        raise RuntimeError("Branch host is missing; ensure branch admin API includes host.")
    if not creds.get("port"):
        raise RuntimeError("Branch port is missing; ensure branch admin API includes port.")
    return TiDBClient.connect(
        host=host,
        port=int(creds.get("port") or DEFAULT_PORT),
        username=creds.get("username"),
        password=creds.get("password"),
        database=creds.get("database") or DEFAULT_DATABASE,
        enable_ssl=True,
    )


def select_branch_rows(branch_name: str, query: str, cluster_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute a SELECT against the given branch and return rows as list-of-dicts."""
    creds = _ensure_admin_credential(branch_name, cluster_id)
    client = _connect_branch_client(creds)
    return client.query(query).to_list()


@tool
def run_sql(sql: str) -> str:
    """
    Execute raw SQL against TiDB.
    Use for DDL, DML, and SELECT queries.
    """
    sql_clean = sql.strip().lower()
    client = _get_tidb_client()
    if sql_clean.startswith("select"):
        rows = client.query(sql).to_list()
        return json.dumps({"rows": rows}, ensure_ascii=False)
    result = client.execute(sql)
    return json.dumps(
        {
            "rowcount": result.rowcount,
            "success": result.success,
            "message": result.message,
        },
        ensure_ascii=False,
    )


@tool
def list_branches() -> str:
    """List branches for the current cluster."""
    branches = _get_branch_manager().list_branches()
    return json.dumps(branches, ensure_ascii=False)


@tool
def create_branch(display_name: str, parent_id: Optional[str] = None) -> str:
    """Create a new branch off the current cluster and wait until ACTIVE."""
    manager = _get_branch_manager()
    created = manager.create_branch(display_name=display_name, parent_id=parent_id)
    branch_id = created.get("branchId") or created.get("id")
    if branch_id:
        _wait_for_branch_active(manager, branch_id)
    return json.dumps(created, ensure_ascii=False)


@tool
def create_branch_from_display_name(display_name: str, parent_display_name: str) -> str:
    """Create a branch with the given display name using the parent branch display name as parent."""
    manager = _get_branch_manager()
    parent_id = _resolve_branch_id(parent_display_name)
    if not parent_id:
        raise RuntimeError(f"Parent branch `{parent_display_name}` not found.")
    created = manager.create_branch(display_name=display_name, parent_id=parent_id)
    branch_id = created.get("branchId") or created.get("id")
    if branch_id:
        _wait_for_branch_active(manager, branch_id)
    return json.dumps(created, ensure_ascii=False)


@tool
def delete_branch(branch_id: str) -> str:
    """Delete a branch by id."""
    manager = _get_branch_manager()
    deleted = manager.delete_branch(branch_id)
    # Clean up cached admin credentials if we can resolve display name
    try:
        info = manager.get_branch(branch_id)
        display_name = info.get("displayName")
        if display_name:
            _delete_admin_credential(display_name, manager._cluster())
    except Exception:
        pass
    return json.dumps(deleted, ensure_ascii=False)


@tool
def reset_branch(branch_id: str) -> str:
    """Reset a branch to its parent state."""
    manager = _get_branch_manager()
    reset = manager.reset_branch(branch_id)
    _wait_for_branch_active(manager, branch_id)
    return json.dumps(reset, ensure_ascii=False)


@tool
def get_branch(branch_id: str) -> str:
    """Get branch details by id."""
    info = _get_branch_manager().get_branch(branch_id)
    return json.dumps(info, ensure_ascii=False)


@tool
def create_branch_backup(display_name: Optional[str] = None) -> str:
    """
    Create a timestamped branch for backup purposes before risky operations.
    If display_name is not provided a default timestamp-based name is used.
    """
    manager = _get_branch_manager()
    backup = manager.create_backup(display_name=display_name)
    branch_id = backup.get("branchId") or backup.get("id")
    if branch_id:
        _wait_for_branch_active(manager, branch_id)
    return json.dumps(backup, ensure_ascii=False)


@tool
def get_admin_user_for_branch(branch_name: str, cluster_id: Optional[str] = None) -> str:
    """
    Retrieve admin credentials for a branch. Uses a local CSV cache before
    creating a new admin account via TiDB Cloud.
    """
    payload = _ensure_admin_credential(branch_name, cluster_id)
    return json.dumps(payload, ensure_ascii=False)


@tool
def run_sql_on_branch(branch_name: str, sql: str) -> str:
    """
    Execute SQL against a specific branch using its admin credentials.
    Supports SELECT (returns rows) and DDL/DML (returns rowcount and message).
    """
    creds = _ensure_admin_credential(branch_name, None)
    client = _connect_branch_client(creds)
    sql_clean = sql.strip().lower()
    if sql_clean.startswith("select"):
        rows = client.query(sql).to_list()
        return json.dumps({"branch": branch_name, "rows": rows}, ensure_ascii=False)
    result = client.execute(sql)
    return json.dumps(
        {
            "branch": branch_name,
            "rowcount": result.rowcount,
            "success": result.success,
            "message": result.message,
        },
        ensure_ascii=False,
    )


def _build_model() -> BedrockModel:
    """Return a Bedrock model using env overrides; defaults to Nova Pro."""
    model_id = (
        os.getenv("STRANDS_MODEL_ID")
        or os.getenv("BEDROCK_MODEL_ID")
        or "us.amazon.nova-pro-v1:0"
    )
    region = os.getenv("BEDROCK_REGION") or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-west-2"
    return BedrockModel(model_id=model_id, region_name=region, streaming=False)


def build_agent() -> Agent:
    """Create the Strands Agent with our TiDB tools."""
    system_prompt = (
        "You are a TiDB Cloud database agent. "
        "You have tools to run SQL (default branch or specific branches) and manage branches. "
        "Always rely on the provided tools; do not invent SQL structure. "
        "Before performing any schema change (DDL) or risky data modification, "
        "ensure there is a fresh branch backup by calling create_branch_backup "
        "or by creating a dedicated branch manually. "
        "For read-only queries you may skip backups. "
        "Use run_sql_on_branch when you need to modify or query a specific branch created during the session. "
        "When creating child branches from an existing branch, use create_branch_from_display_name (preferred) "
        "or create_branch with parent_id resolved from the chosen parent; do not branch from the default/main "
        "when the user has selected a specific parent."
    )
    return Agent(
        model=_build_model(),
        tools=[
            run_sql,
            list_branches,
            create_branch,
            create_branch_from_display_name,
            delete_branch,
            reset_branch,
            get_branch,
            create_branch_backup,
            get_admin_user_for_branch,
            run_sql_on_branch,
        ],
        system_prompt=system_prompt,
    )


class _BranchManagerProxy:
    """Provide backward-compatible attribute access while keeping lazy init semantics."""

    def __getattr__(self, item: str) -> Any:
        return getattr(_get_branch_manager(), item)


# Backward-compatible handle used by Streamlit and notebooks.
branch_manager = _BranchManagerProxy()

# Reusable agent instance for non-Streamlit contexts.
agent = build_agent()
