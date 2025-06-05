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
  } from "@xyflow/svelte";

  import "@xyflow/svelte/dist/style.css";
  import SvelteSelect from "svelte-select";

  let nodes: Node[] = [];
  let edges: Edge[] = [];

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

  function deleteNode() {
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;
    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    
    selectedNodeIds.forEach(async (nodeId) => {
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
    });

    loadSelectedPipeline();

    // nodes = nodes.filter((node) => !selectedNodeIds.includes(node.id));

    // edges = edges.filter(
    //   (edge) =>
    //     !selectedNodeIds.includes(edge.source) &&
    //     !selectedNodeIds.includes(edge.target),
    // );
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

  async function loadSelectedPipeline() {
    const pipelineId = typeof selectedPipeline === "string" ? selectedPipeline : selectedPipeline.value;

    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(`http://localhost:8000/get_pipeline/${pipelineId}`);
      if (!response.ok) {
        throw new Error(`Failed to load selected pipeline: ${response.statusText}`);
      }

      const pipeline = await response.json();

      const rawNodes = Object.entries(pipeline.nodes).map(([id, node]) => ({
        id,
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
    />
    <button onclick={loadSelectedPipeline} style="margin-right: 10px;"> Load pipeline </button>
    
  </div>

  <SvelteFlow bind:nodes bind:edges fitView>
    <Panel position="center-left">
      Example of pipeline id information box
    </Panel>

    <Controls />
    <Background />
    <MiniMap />
  </SvelteFlow>
</div>
