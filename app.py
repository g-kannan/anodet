import gradio as gr
from databricks.sdk import WorkspaceClient
import json

def list_compute_resources(host_url, token):
    try:
        if not host_url or not token:
            return json.dumps({"error": "Please provide both host URL and token"}, indent=2)
        
        w = WorkspaceClient(host=host_url, token=token)
        
        # Get clusters
        clusters = list(w.clusters.list())
        cluster_info = []
        for cluster in clusters:
            cluster_info.append({
                "type": "Cluster",
                "name": cluster.cluster_name or "N/A",
                "id": cluster.cluster_id or "N/A",
                "state": cluster.state.value if cluster.state else "Unknown",
                "spark_version": cluster.spark_version or "N/A"
            })
        
        # Get SQL warehouses
        warehouses = list(w.warehouses.list())
        warehouse_info = []
        for warehouse in warehouses:
            warehouse_info.append({
                "type": "SQL Warehouse",
                "name": warehouse.name or "N/A",
                "id": warehouse.id or "N/A",
                "state": warehouse.state.value if warehouse.state else "Unknown",
                "cluster_size": warehouse.cluster_size or "N/A",
                "warehouse_type": warehouse.warehouse_type.value if warehouse.warehouse_type else "N/A"
            })
        
        result = {
            "clusters": cluster_info,
            "sql_warehouses": warehouse_info,
            "summary": {
                "total_clusters": len(cluster_info),
                "total_warehouses": len(warehouse_info)
            }
        }
        
        if not cluster_info and not warehouse_info:
            return json.dumps({"message": "No compute resources found"}, indent=2)
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)}, indent=2)

with gr.Blocks() as demo:
    gr.Markdown("# Databricks Compute Resources Viewer")
    gr.Markdown("View all clusters and SQL warehouses in your Databricks workspace")
    
    with gr.Row():
        host_input = gr.Textbox(
            label="Databricks Workspace URL", 
            placeholder="https://your-workspace.cloud.databricks.com"
        )
        token_input = gr.Textbox(
            label="Token", 
            type="password", 
            placeholder="Enter your access token"
        )
    
    list_btn = gr.Button("List Compute Resources")
    output = gr.JSON(label="Compute Resources")
    
    list_btn.click(fn=list_compute_resources, inputs=[host_input, token_input], outputs=output)

if __name__ == "__main__":
    demo.launch()