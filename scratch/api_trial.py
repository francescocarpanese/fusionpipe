import requests

url = "http://localhost:8000/create_pipeline"
payload = {}
response = requests.post(url)

print("Status code:", response.status_code)
print("Response:", response.json())


# Add a node to the created pipeline
pipeline_id = response.json().get("pipeline_id")
if pipeline_id:
    node_url = f"http://localhost:8000/add_node"
    node_payload = {
        "pipeline_id": pipeline_id,
    }
    node_response = requests.post(node_url, json=node_payload)
    print("Add node status code:", node_response.status_code)
    print("Add node response:", node_response.json())
else:
    print("Failed to retrieve pipeline_id from response.")
    # Try again to add a node using the correct API endpoint and payload
    add_node_url = "http://localhost:8000/add_node"
    add_node_payload = {
        "pipeline_id": pipeline_id
    }
    add_node_response = requests.post(add_node_url, json=add_node_payload)
    print("Add node (correct API) status code:", add_node_response.status_code)
    print("Add node (correct API) response:", add_node_response.json())