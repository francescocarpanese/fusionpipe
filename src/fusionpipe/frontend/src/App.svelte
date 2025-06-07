<script lang="ts">
  import dagre from "@dagrejs/dagre";
  import {
    SvelteFlow,
    Controls,
    Background,
    MiniMap,
    Position,
    Panel,
    type Node,
    type Edge,
    type OnConnect,
  } from "@xyflow/svelte";

  import "@xyflow/svelte/dist/style.css";
  import SvelteSelect from "svelte-select";
  import TextUpdaterNode from './TextUpdaterNode.svelte';
  import CustomNode from './CustomNode.svelte';

  let nodes: Node[] = [];
  let edges: Edge[] = [];
  let nodeTypes = {
    custom: CustomNode,
  };

  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  const nodeWidth = 172;
  const nodeHeight = 36;

  function getLayoutedElements(nodes: Node[], edges: Edge[], direction = "TB") {
    const isHorizontal = direction === "LR";
    dagreGraph.setGraph({ rankdir: direction });

    nodes.forEach((node) => {
      dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
    });

    edges.forEach((edge) => {
      dagreGraph.setEdge(edge.source, edge.target);
    });

    dagre.layout(dagreGraph);

    const layoutedNodes = nodes.map((node) => {
      const nodeWithPosition = dagreGraph.node(node.id);
      node.targetPosition = isHorizontal ? Position.Left : Position.Top;
      node.sourcePosition = isHorizontal ? Position.Right : Position.Bottom;

      return {
        ...node,
        position: {
          x: nodeWithPosition.x - nodeWidth / 2,
          y: nodeWithPosition.y - nodeHeight / 2,
        },
      };
    });

    return { nodes: layoutedNodes, edges };
  }

  function onLayout(direction: string) {
    const layoutedElements = getLayoutedElements(nodes, edges, direction);
    nodes = layoutedElements.nodes;
    edges = layoutedElements.edges;
  }


  function getNodeColor(status: string): string {
    switch (status) {
      case "ready":
        return "#808080"; // gray
      case "running":
        return "#0000FF"; // blue
      case "completed":
        return "#008000"; // green
      case "failed":
        return "#FF0000"; // red
      case "staledata":
        return "#FFFF00"; // yellow
      default:
        return "#FFFFFF"; // white
    }
  }

  async function addNode() {
    try {

      const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
  
      if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

      const response = await fetch(`http://localhost:8000/create_node_in_pipeline/${pipelineId}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ pipeline_id: pipelineId }),
      });

      if (!response.ok) {
        throw new Error(`Failed to add node: ${response.statusText}`);
      }

      loadSelectedPipeline();
    } catch (error) {
      console.error("Error adding node:", error);
    }
  }

  async function deleteNode() {
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    
      await Promise.all(selectedNodeIds.map(async (nodeId) => {
        try {
          const response = await fetch(`http://localhost:8000/delete_node_from_pipeline/${pipelineId}/${nodeId}`, {
            method: "DELETE",
            headers: {
              "Content-Type": "application/json",
            },
          });

          if (!response.ok) {
            throw new Error(`Failed to delete node ${nodeId}: ${response.statusText}`);
          }
        } catch (error) {
          console.error(`Error deleting node ${nodeId}:`, error);
        }
      }));


    await loadPipeline(pipelineId);
  }


  let pipelines: string[] = [];
  let selectedPipeline = "";

  async function fetchPipelines() {
    try {
      const response = await fetch("http://localhost:8000/get_all_pipeline_ids");
      if (!response.ok) {
        throw new Error(`Failed to fetch pipelines: ${response.statusText}`);
      }
      const data = await response.json();
      pipelines = data.pip_ids;

    } catch (error) {
      console.error("Error fetching pipelines:", error);
    }
  }

  async function handleConnect(event) {
    const source = event.source;
    const target = event.target;
    console.log(`Connected nodes: ${source} → ${target}`);

    // API call to connect the nodes
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }
    try {
      const response = await fetch(`http://localhost:8000/connect_nodes`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ source, target }),
      });
      if (!response.ok) {
      throw new Error(`Failed to connect nodes: ${response.statusText}`);
      }
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error connecting nodes:", error);
    }

  }


  async function createPipeline() {
    try {
      const response = await fetch("http://localhost:8000/create_pipeline", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`Failed to create pipeline: ${response.statusText}`);
      }
      const data = await response.json();
      const newPipelineId = data.pipeline_id || data.id || data.pip_id;
      await fetchPipelines();
      selectedPipeline = newPipelineId;
      await loadPipeline(newPipelineId);
    } catch (error) {
      console.error("Error creating pipeline:", error);
    }
  }


  async function deleteSelectedPipeline() {
    if (!selectedPipeline) {
      alert("No pipeline selected");
      return;
    }
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    const confirmed = confirm(`Are you sure you want to delete pipeline "${pipelineId}"? This action cannot be undone.`);
    if (!confirmed) return;

    try {
      const response = await fetch(`http://localhost:8000/delete_pipeline/${pipelineId}`, {
        method: "DELETE",
        headers: {
          "Content-Type": "application/json",
        },
      });
      if (!response.ok) {
        throw new Error(`Failed to delete pipeline: ${response.statusText}`);
      }
      await fetchPipelines();
      selectedPipeline = "";
      nodes = [];
      edges = [];
    } catch (error) {
      console.error("Error deleting pipeline:", error);
      alert("Failed to delete pipeline.");
    }
  }


  async function branchPipelineFromNode() {
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      console.error("No node selected");
      return;
    }

    const startNodeId = selectedNode.id;

    try {
      const response = await fetch(`http://localhost:8000/branch_pipeline/${pipelineId}/${startNodeId}`, {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to branch pipeline: ${response.statusText}`);
      }

      const data = await response.json();
      console.log("Pipeline iteration result:", data);

      await fetchPipelines();
      await loadPipeline(data.new_pipeline);
      alert(`Pipeline iteration completed.\nStart Node: ${startNodeId}\nSource Pipeline: ${pipelineId}\nNew Pipeline ID: ${data.new_pipeline}`);

    } catch (error) {
      console.error("Error iterating pipeline:", error);
      alert("Failed to branch pipeline.");
    }
  }


  async function loadPipeline(pipelineId: string) {

    try {
      const response = await fetch(`http://localhost:8000/get_pipeline/${pipelineId}`, {
        cache: "no-store"
      });
      if (!response.ok) {
        throw new Error(`Failed to load selected pipeline: ${response.statusText}`);
      }

      const pipeline = await response.json();

      const rawNodes = Object.entries(pipeline.nodes).map(([id, node]) => ({
        id,
        type: 'custom',
        data: {
          label: `${id}\n ${node.tag}`,
          editable: node.editable,
        },
        position: { x: Math.random() * 400, y: Math.random() * 400 },
        style: `background: ${getNodeColor(node.status)}`,
      }));

      const rawEdges = Object.entries(pipeline.nodes)
        .flatMap(([id, node]) =>
          node.parents.map((parentId) => ({
            id: `${parentId}-${id}`,
            source: parentId,
            target: id,
          }))
        );

      const layoutedElements = getLayoutedElements(rawNodes, rawEdges);
      // Reassign the arrays to trigger reactivity
      nodes = [...layoutedElements.nodes];
      edges = [...layoutedElements.edges];

    } catch (error) {
      console.error("Error loading selected pipeline:", error);
    }
  }



  async function loadSelectedPipeline() {
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }
    await loadPipeline(pipelineId);
  }


  $: fetchPipelines();
</script>



<style>
  .node-content {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    position: relative;
  }

  .lock-icon {
    position: absolute;
    top: 5px;
    right: 5px;
    font-size: 12px;
    color: black;
  }
</style>

<div style:height="100vh">
  <div style="position: absolute; top: 0; left: 0; width: 100%; background-color: #f4f4f4; padding: 10px; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); z-index: 10;">
    <button onclick={addNode} style="margin-right: 10px;"> Add Node </button>
    <button onclick={deleteNode} style="margin-right: 10px;"> Delete Node </button>
    <button onclick={() => onLayout('TB')} style="margin-right: 10px;"> Vertical Layout </button>
    <button onclick={() => onLayout('LR')} style="margin-right: 10px;"> Horizontal Layout </button>
    <SvelteSelect
      items={pipelines}
      bind:value={selectedPipeline}
      placeholder="Search or select a pipeline..."
      maxItems={5}
      on:select={loadSelectedPipeline}
    />
    <button onclick={createPipeline} style="margin-right: 10px;"> Create pipeline </button>
    <button onclick={deleteSelectedPipeline} style="margin-right: 10px;"> Delete pipeline </button>
    <button onclick={branchPipelineFromNode} style="margin-right: 10px;"> Branch Pipeline from node </button>

    
  </div>

  <SvelteFlow bind:nodes bind:edges fitView onconnect={handleConnect} {nodeTypes}>
    <Panel position="center-left">
      Example of pipeline id information box
    </Panel>

    <Controls />
    <Background />
    <MiniMap />
  </SvelteFlow>
</div>
