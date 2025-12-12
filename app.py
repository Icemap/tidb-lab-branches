from typing import Any, Dict, List

import streamlit as st
from dotenv import load_dotenv

from tidb_agent import agent, branch_manager

load_dotenv()

st.set_page_config(page_title="TiDB Strands Agent Demo", page_icon="🦾", layout="wide")
st.title("TiDB Cloud Agent Demo (Strands + Streamlit)")
st.write(
    "Natural language control of TiDB Cloud for schema changes, CRUD, and branch management."
)


def run_agent(prompt: str) -> Dict[str, Any]:
    """Execute the agent and return the agent result as dict for display."""
    result = agent(prompt)
    return {
        "message": str(result).strip(),
        "metrics": result.metrics.get_summary() if result.metrics else {},
        "raw": result.to_dict(),
    }


with st.sidebar:
    st.header("Instructions")
    default_prompt = (
        "Create a table named users with an auto-incrementing id and a name column. "
        "Insert a few demo rows and select all rows."
    )
    user_prompt = st.text_area("Describe what you want the agent to do", value=default_prompt, height=180)
    run_btn = st.button("Run Agent", type="primary")
    st.caption(
        "When your request involves schema or data changes, the agent should first create a branch backup using the provided tools."
    )

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Agent Output")
    if run_btn:
        with st.spinner("Agent is reasoning and calling tools..."):
            try:
                agent_result = run_agent(user_prompt)
                st.success(agent_result["message"] or "Completed.")
                with st.expander("Tool metrics", expanded=False):
                    st.json(agent_result["metrics"])
                with st.expander("Raw AgentResult payload", expanded=False):
                    st.json(agent_result["raw"])
            except Exception as exc:
                st.error(f"Execution failed: {exc}")
    else:
        st.info("Describe a task and click Run Agent.")

with col2:
    st.subheader("Cluster Branches")
    try:
        branches = branch_manager.list_branches()
        if not branches:
            st.write("No branches returned.")
        else:
            rows: List[Dict[str, Any]] = []
            for b in branches:
                rows.append(
                    {
                        "branchId": b.get("branchId"),
                        "displayName": b.get("displayName"),
                        "parentId": b.get("parentId"),
                        "state": b.get("state"),
                        "createTime": b.get("createTime"),
                        "updateTime": b.get("updateTime"),
                    }
                )
            st.dataframe(rows, use_container_width=True)
    except Exception as exc:
        st.error(f"Failed to load branches: {exc}")
