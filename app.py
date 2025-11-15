import gradio as gr
from databricks.sdk import WorkspaceClient
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

theme=gr.themes.Ocean()

def list_compute_resources(host_url, token):
    try:
        # Use .env values if inputs are empty
        host_url = host_url or os.getenv("url", "")
        token = token or os.getenv("token", "")
        
        if not host_url or not token:
            error_msg = [["Error", "Please provide both host URL and token"]]
            return error_msg, error_msg, json.dumps({"error": "Please provide both host URL and token (via input or .env file)"}, indent=2)
        
        w = WorkspaceClient(host=host_url, token=token)
        
        # Get clusters
        clusters = list(w.clusters.list())
        cluster_info = []
        cluster_states = {}
        for cluster in clusters:
            state = cluster.state.value if cluster.state else "Unknown"
            cluster_states[state] = cluster_states.get(state, 0) + 1
            cluster_info.append({
                "type": "Cluster",
                "name": cluster.cluster_name or "N/A",
                "id": cluster.cluster_id or "N/A",
                "state": state,
                "spark_version": cluster.spark_version or "N/A"
            })
        
        # Get SQL warehouses
        warehouses = list(w.warehouses.list())
        warehouse_info = []
        warehouse_states = {}
        for warehouse in warehouses:
            state = warehouse.state.value if warehouse.state else "Unknown"
            warehouse_states[state] = warehouse_states.get(state, 0) + 1
            warehouse_info.append({
                "type": "SQL Warehouse",
                "name": warehouse.name or "N/A",
                "id": warehouse.id or "N/A",
                "state": state,
                "cluster_size": warehouse.cluster_size or "N/A",
                "warehouse_type": warehouse.warehouse_type.value if warehouse.warehouse_type else "N/A",
                "auto_stop_mins": warehouse.auto_stop_mins if warehouse.auto_stop_mins is not None else "N/A"
            })
        
        # Create cluster summary table
        cluster_summary = [
            ["Total Clusters", len(cluster_info)]
        ]
        if cluster_states:
            for state, count in sorted(cluster_states.items()):
                cluster_summary.append([state, count])
        
        # Create warehouse summary table
        warehouse_summary = [
            ["Total Warehouses", len(warehouse_info)]
        ]
        if warehouse_states:
            for state, count in sorted(warehouse_states.items()):
                warehouse_summary.append([state, count])
        
        result = {
            "clusters": cluster_info,
            "sql_warehouses": warehouse_info,
            "summary": {
                "total_clusters": len(cluster_info),
                "total_warehouses": len(warehouse_info),
                "cluster_states": cluster_states,
                "warehouse_states": warehouse_states
            }
        }
        
        if not cluster_info and not warehouse_info:
            empty = [["No resources found", "0"]]
            return empty, empty, json.dumps({"message": "No compute resources found"}, indent=2)
        
        return cluster_summary, warehouse_summary, json.dumps(result, indent=2)
    except Exception as e:
        error = [[str(e), ""]]
        return error, error, json.dumps({"error": str(e)}, indent=2)

with gr.Blocks() as demo:
    gr.Markdown("# Databricks Compute Resources Viewer")
    gr.Markdown("View all clusters and SQL warehouses in your Databricks workspace")
    gr.Markdown("💡 **Tip:** Leave fields empty to use values from `.env` file")
    
    with gr.Row():
        host_input = gr.Textbox(
            label="Databricks Workspace URL", 
            placeholder="https://your-workspace.cloud.databricks.com (or leave empty for .env)",
            value=os.getenv("url", "")
        )
        token_input = gr.Textbox(
            label="Token", 
            type="password", 
            placeholder="Enter your access token (or leave empty for .env)",
            value=os.getenv("token", "")
        )
    
    list_btn = gr.Button("List Compute Resources")
    
    with gr.Row():
        cluster_table = gr.Dataframe(
            label="Clusters Summary",
            headers=["Metric", "Count"],
            datatype=["str", "number"],
            col_count=2
        )
        warehouse_table = gr.Dataframe(
            label="SQL Warehouses Summary",
            headers=["Metric", "Count"],
            datatype=["str", "number"],
            col_count=2
        )
    
    with gr.Accordion("Full Details (JSON)", open=False):
        json_output = gr.JSON(label="Complete Resource Details")
    
    list_btn.click(
        fn=list_compute_resources, 
        inputs=[host_input, token_input], 
        outputs=[cluster_table, warehouse_table, json_output]
    )

if __name__ == "__main__":
    demo.launch()