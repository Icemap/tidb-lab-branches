# TiDB Cloud Strands Agent Demo

Minimal example showing how to combine the Amazon Strands Agent SDK with TiDB Cloud and Streamlit. The agent can run arbitrary SQL (DDL/DML/SELECT) and manage cluster branches. Instead of automatically backing up, the model prompt instructs the agent to create a branch backup (via `create_branch_backup` or `create_branch`) before any schema change or risky data modification.

## Prerequisites

1. Python 3.12+ and [uv](https://github.com/astral-sh/uv) installed.
2. TiDB Cloud credentials for both SQL access and the Serverless API.

## Setup

```bash
uv venv --python python3
source .venv/bin/activate
uv sync
cp example.env .env
```

Update `.env` with:

- `SERVERLESS_CLUSTER_HOST`, `SERVERLESS_CLUSTER_PORT`, `SERVERLESS_CLUSTER_USERNAME`, `SERVERLESS_CLUSTER_PASSWORD`, `SERVERLESS_CLUSTER_DATABASE_NAME`
- `TIDBCLOUD_ACCESS_TOKEN` (preferred) **or** `TIDBCLOUD_PUBLIC_KEY` + `TIDBCLOUD_PRIVATE_KEY`
- `SERVERLESS_CLUSTER_ID` (optional; if omitted the first cluster returned by the API is used)
- `TIDBCLOUD_PROJECT_ID` (required to create per-branch admin credentials)

## Run the Streamlit UI

```bash
uv run streamlit run app.py
```

The sidebar accepts natural language requests; the agent result (including tool metrics and raw payload) is shown on the left, and the current list of branches is displayed on the right.

## Implementation Notes

- `tidb_agent.py`
  - Loads environment variables, connects to TiDB via `pytidb`, and wraps the TiDB Cloud branch API (supports bearer tokens or HTTP Digest).  
  - Exposes tools: `run_sql`, `list_branches`, `create_branch`, `delete_branch`, `reset_branch`, `get_branch`, `create_branch_backup`, and `get_admin_user_for_branch`.  
  - `get_admin_user_for_branch` stores credentials in `branch_credentials.csv` and reuses cached usernames/passwords before creating new ones via the TiDB Cloud internal API.  
  - The Strands agent prompt reminds the model to call `create_branch_backup` (or create a manual branch) before DDL or other high-risk updates.
- `app.py`
  - Minimal Streamlit UI for triggering the agent and visualizing branch state.

## Notebook storyline

`todo_branch_story.ipynb` walks through the branching workflow described in the prompt: generate three competing todo schemas, pick one interactively, explore three user-aware variations, and visualize the branch tree. Open it in Jupyter (or VS Code / JupyterLab) after configuring the project environment.

This is a demo, so error handling and security are intentionally lightweight—extend as needed before using in production.
