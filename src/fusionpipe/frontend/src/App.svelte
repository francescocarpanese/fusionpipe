<script lang="ts">
  import "./app.css";
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
  import TextUpdaterNode from "./TextUpdaterNode.svelte";
  import CustomNode from "./CustomNode.svelte";
  import { Drawer, Button, CloseButton, Label, Input } from "flowbite-svelte";
  import { InfoCircleSolid, ArrowRightOutline } from "flowbite-svelte-icons";
  import { sineIn } from "svelte/easing";
  import {
    Dropdown,
    DropdownItem,
    DropdownDivider,
    Navbar,
    NavBrand,
    NavHamburger,
    NavUl,
    NavLi,
  } from "flowbite-svelte";
  import { ChevronDownOutline } from "flowbite-svelte-icons";

  // Variables and state definitions
  let nodes = $state<Node[]>([]);
  let edges = $state([]);
  let selectedPipeline = $state(null);
  const nodeWidth = 172;
  const nodeHeight = 36;
  let nodeDrawereForm = $state({
    id: "",
    tag: "",
    notes: "",
  });

  let pipelineDrawerForm = $state({
    id: "",
    tag: "",
    notes: "",
  });

  let isHiddenPipelinePanel = $state(true);
  let isHiddenNodePanel = $state(true);
  let pipelines = $state([]);

  let nodeTypes = { custom: CustomNode };
  const dagreGraph = new dagre.graphlib.Graph();

  dagreGraph.setDefaultEdgeLabel(() => ({}));

  // Define functions
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

  function getNodeColor(status: string): string {
    switch (status) {
      case "ready":
        return "#808080";

      case "running":
        return "#0000FF";

      case "completed":
        return "#008000";

      case "failed":
        return "#FF0000";

      case "staledata":
        return "#FFFF00";

      default:
        return "#FFFFFF";
    }
  }

  async function addNode() {
    try {
      const pipelineId =
        typeof selectedPipeline === "string"
          ? selectedPipeline
          : selectedPipeline.value;

      if (!selectedPipeline) {
        console.error("No pipeline selected");
        return;
      }

      const response = await fetch(
        `http://localhost:8000/create_node_in_pipeline/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pipeline_id: pipelineId }),
        },
      );

      if (!response.ok) {
        throw new Error(`Failed to add node: ${response.statusText}`);
      }

      loadSelectedPipeline();
    } catch (error) {
      console.error("Error adding node:", error);
    }
  }

  async function deleteNode() {
    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;

    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    await Promise.all(
      selectedNodeIds.map(async (nodeId) => {
        try {
          const response = await fetch(
            `http://localhost:8000/delete_node_from_pipeline/${pipelineId}/${nodeId}`,
            {
              method: "DELETE",
              headers: { "Content-Type": "application/json" },
            },
          );

          if (!response.ok) {
            throw new Error(
              `Failed to delete node ${nodeId}: ${response.statusText}`,
            );
          }
        } catch (error) {
          console.error(`Error deleting node ${nodeId}:`, error);
        }
      }),
    );

    await loadPipeline(pipelineId);
  }

  async function deleteEdge() {
    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;

    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    const selectedEdgeIds = edges
      .filter((edge) => edge.selected)
      .map((edge) => edge.id);

    await Promise.all(
      selectedEdgeIds.map(async (edgeId) => {
        const edge = edges.find((e) => e.id === edgeId);

        if (!edge) return;

        try {
          const response = await fetch(
            `http://localhost:8000/delete_edge/${pipelineId}/${edge.source}/${edge.target}`,
            {
              method: "DELETE",
              headers: { "Content-Type": "application/json" },
            },
          );

          if (!response.ok) {
            throw new Error(
              `Failed to delete edge ${edgeId}: ${response.statusText}`,
            );
          }
        } catch (error) {
          console.error(`Error deleting edge ${edgeId}:`, error);
        }
      }),
    );

    await loadPipeline(pipelineId);
  }

  async function fetchPipelines() {
    try {
      const response = await fetch(
        "http://localhost:8000/get_all_pipeline_ids",
      );

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

    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;

    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(`http://localhost:8000/connect_nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
        headers: { "Content-Type": "application/json" },
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

    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;
    const confirmed = confirm(
      `Are you sure you want to delete pipeline "${pipelineId}"? This action cannot be undone.`,
    );

    if (!confirmed) return;

    try {
      const response = await fetch(
        `http://localhost:8000/delete_pipeline/${pipelineId}`,
        {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
        },
      );

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
    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;

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
      const response = await fetch(
        `http://localhost:8000/branch_pipeline/${pipelineId}/${startNodeId}`,
        {
          method: "GET",
          headers: { "Content-Type": "application/json" },
        },
      );

      if (!response.ok) {
        throw new Error(`Failed to branch pipeline: ${response.statusText}`);
      }

      const data = await response.json();

      console.log("Pipeline iteration result:", data);
      await fetchPipelines();
      await loadPipeline(data.new_pipeline);
      alert(
        `Pipeline iteration completed.\nStart Node: ${startNodeId}\nSource Pipeline: ${pipelineId}\nNew Pipeline ID: ${data.new_pipeline}`,
      );
    } catch (error) {
      console.error("Error iterating pipeline:", error);
      alert("Failed to branch pipeline.");
    }
  }

  async function loadPipeline(pipelineId: string) {
    try {
      const response = await fetch(
        `http://localhost:8000/get_pipeline/${pipelineId}`,
        { cache: "no-store" },
      );

      if (!response.ok) {
        throw new Error(
          `Failed to load selected pipeline: ${response.statusText}`,
        );
      }

      const pipeline = await response.json();

      const rawNodes = Object.entries(pipeline.nodes).map(([id, node]) => ({
        id,
        type: "custom",
        data: {
          label: `${node.tag}`,
          editable: node.editable,
          notes: node.notes || "",
        },
        position: {
          x: Math.random() * 400,
          y: Math.random() * 400,
        },
        style: `background: ${getNodeColor(node.status)}`,
      }));

      const rawEdges = Object.entries(pipeline.nodes).flatMap(([id, node]) =>
        node.parents.map((parentId) => ({
          id: `${parentId}-${id}`,
          source: parentId,
          target: id,
        })),
      );

      const layoutedElements = getLayoutedElements(rawNodes, rawEdges);

      nodes = [...layoutedElements.nodes];
      edges = [...layoutedElements.edges];
    } catch (error) {
      console.error("Error loading selected pipeline:", error);
    }
  }

  async function loadSelectedPipeline() {
    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;

    if (!selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }

    await loadPipeline(pipelineId);
  }

  // Add refs for the input fields

  async function updateNodeInfo() {
    if (!nodeDrawereForm || !selectedPipeline) {
      console.error("No node or pipeline selected");
      return;
    }

    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;
    const nodeId = nodeDrawereForm.id;
    const newTag = nodeDrawereForm.tag;
    const newNotes = nodeDrawereForm.notes;

    try {
      // Update node tag
      await fetch(
        `http://localhost:8000/update_node_tag/${pipelineId}/${nodeId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ node_tag: newTag }),
        },
      );

      // Update node notes
      await fetch(
        `http://localhost:8000/update_node_notes/${pipelineId}/${nodeId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ notes: newNotes }),
        },
      );

      await loadPipeline(pipelineId);
      alert("Node info updated.");
    } catch (error) {
      console.error("Error updating node info:", error);
      alert("Failed to update node info.");
    }
  }

  // Add a function to fetch pipeline info
  async function loadPipelineInfo(pipelineId: string) {
    try {
      const response = await fetch(
        `http://localhost:8000/get_pipeline/${pipelineId}`,
        { cache: "no-store" }
      );
      if (!response.ok) {
        throw new Error(`Failed to load pipeline info: ${response.statusText}`);
      }
      const pipeline = await response.json();
      pipelineDrawerForm = {
        id: pipeline.pipeline_id || "",
        tag: pipeline.tag || "",
        notes: pipeline.notes || "",
      };
    } catch (error) {
      console.error("Error loading pipeline info:", error);
      pipelineDrawerForm = { id: "", tag: "", notes: "" };
    }
  }

  // Add a function to update pipeline info
  async function updatePipelineInfo() {
    if (!pipelineDrawerForm || !selectedPipeline) {
      console.error("No pipeline selected");
      return;
    }
    const pipelineId =
      typeof selectedPipeline === "string"
        ? selectedPipeline
        : selectedPipeline.value;
    const newTag = pipelineDrawerForm.tag;
    const newNotes = pipelineDrawerForm.notes;

    try {
      await fetch(
        `http://localhost:8000/update_pipeline_tag/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ tag: newTag }),
        }
      );
      await fetch(
        `http://localhost:8000/update_pipeline_notes/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ notes: newNotes }),
        }
      );
      await fetchPipelines();
      await loadPipeline(pipelineId);
      alert("Pipeline info updated.");
    } catch (error) {
      console.error("Error updating pipeline info:", error);
      alert("Failed to update pipeline info.");
    }
  }

  // Collection of all reactive effects
  $effect(() => {
    if (!isHiddenNodePanel) {
      const selectedNodes = nodes.filter((node) => node.selected);
      if (selectedNodes.length === 1) {
        const selectedNode = selectedNodes[0];
        nodeDrawereForm = {
          id: selectedNode.id,
          tag: selectedNode.data?.label || "No tag",
          notes: selectedNode.data?.notes || "",
        };
      } else {
        nodeDrawereForm = {
          id: "",
          tag: "",
          notes: "",
        };
      }
    } else {
      nodeDrawereForm = {
        id: "",
        tag: "",
        notes: "",
      };
    }
  });

  // Effect to load pipeline info when the panel is opened
  $effect(() => {
    if (!isHiddenPipelinePanel && selectedPipeline) {
      const pipelineId =
        typeof selectedPipeline === "string"
          ? selectedPipeline
          : selectedPipeline.value;
      loadPipelineInfo(pipelineId);
    }
    if (isHiddenPipelinePanel) {
      pipelineDrawerForm = { id: "", tag: "", notes: "" };
    }
  });

  $effect(fetchPipelines);
</script>

<div class="app-layout">
  <Navbar>
    <NavUl class="ms-3 pt-1">
      <NavLi class="cursor-pointer">
        Pipeline Interaction<ChevronDownOutline
          class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
        />
      </NavLi>
      <Dropdown simple>
        <DropdownItem>
          <SvelteSelect
            items={pipelines}
            bind:value={selectedPipeline}
            placeholder="Select a pipeline..."
            maxItems={5}
            on:select={loadSelectedPipeline}
          />
        </DropdownItem>
        <DropdownItem onclick={() => (isHiddenPipelinePanel = false)}
          >Open selected pipeline panel</DropdownItem
        >
        <DropdownItem onclick={createPipeline}>Create Pipeline</DropdownItem>
        <DropdownItem onclick={branchPipelineFromNode}
          >Branch Pipeline from selected node</DropdownItem
        >
        <DropdownItem onclick={deleteSelectedPipeline}
          >Delete Pipeline</DropdownItem
        >
      </Dropdown>
      <NavLi class="cursor-pointer">
        Node interaction<ChevronDownOutline
          class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
        />
      </NavLi>
      <Dropdown simple>
        <DropdownItem onclick={() => (isHiddenNodePanel = false)}
          >Open selected node panel</DropdownItem
        >
        <DropdownItem onclick={addNode}>Create node</DropdownItem>
        <DropdownItem class="text-gray-400 cursor-not-allowed"
          >Copy selected nodes</DropdownItem
        >
        <DropdownItem onclick={deleteNode}>Delete selected nodes</DropdownItem>
        <DropdownItem onclick={deleteEdge}>Delete selected edge</DropdownItem>
      </Dropdown>
      <NavLi class="cursor-pointer">
        Actions<ChevronDownOutline
          class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
        />
      </NavLi>
      <Dropdown simple>
        <DropdownItem class="text-gray-400 cursor-not-allowed"
          >Run pipeline</DropdownItem
        >
        <DropdownItem class="text-gray-400 cursor-not-allowed"
          >Run up to selected node</DropdownItem
        >
        <DropdownItem class="text-gray-400 cursor-not-allowed"
          >Open run panel</DropdownItem
        >
      </Dropdown>
      <NavLi class="cursor-pointer">
        Layout<ChevronDownOutline
          class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
        />
      </NavLi>
      <Dropdown simple>
        <DropdownItem>Vertical</DropdownItem>
        <DropdownItem>Horizontal</DropdownItem>
        <DropdownItem class="text-gray-400 cursor-not-allowed"
          >Auto reshape</DropdownItem
        >
      </Dropdown>
    </NavUl>
  </Navbar>

  <Drawer
    bind:hidden={isHiddenNodePanel}
    id="nodesidebar"
    aria-controls="nodesidebar"
    aria-labelledby="nodesidebar"
  >
    <div class="flex items-center justify-between">
      <CloseButton
        onclick={() => (isHiddenNodePanel = false)}
        class="mb-4 dark:text-white"
      />
    </div>
    {#if nodeDrawereForm}
      <Label for="node_tag" class="mb-2 block">Node id:</Label>
      <div class="mt-2 text-sm text-gray-500">
        {nodeDrawereForm.id}
      </div>
      <Label for="node_tag" class="mb-2 block">Node tag:</Label>
      <Input
        id="node_tag"
        name="node_tag"
        required
        bind:value={nodeDrawereForm.tag}
      />
      <Label for="node_notes" class="mb-2 block">Notes:</Label>
      <Input
        id="node_notes"
        name="node_notes"
        required
        bind:value={nodeDrawereForm.notes}
      />
      <Button onclick={updateNodeInfo} class="mt-4">Save Changes</Button>
    {/if}
  </Drawer>

  <Drawer
    bind:hidden={isHiddenPipelinePanel}
    id="pipelinesidebar"
    aria-controls="pipelinesidebar"
    aria-labelledby="pipelinesidebar"
  >
    <div class="flex items-center justify-between">
      <CloseButton
        onclick={() => (isHiddenPipelinePanel = false)}
        class="mb-4 dark:text-white"
      />
    </div>
    {#if pipelineDrawerForm}
      <Label class="mb-2 block">Pipeline id:</Label>
      <div class="mt-2 text-sm text-gray-500">
        {pipelineDrawerForm.id}
      </div>
      <Label for="pipeline_tag" class="mb-2 block">Pipeline tag:</Label>
      <Input
        id="pipeline_tag"
        name="pipeline_tag"
        required
        bind:value={pipelineDrawerForm.tag}
      />
      <Label for="pipeline_notes" class="mb-2 block">Notes:</Label>
      <Input
        id="pipeline_notes"
        name="pipeline_notes"
        required
        bind:value={pipelineDrawerForm.notes}
      />
      <Button onclick={updatePipelineInfo} class="mt-4">Save Changes</Button>
    {/if}
  </Drawer>

  <div class="main-content">
    <SvelteFlow
      bind:nodes
      bind:edges
      fitView
      onconnect={handleConnect}
      {nodeTypes}
      style="height: 100%;"
      disableKeyboardA11y={true}
    >
      <Controls />
      <Background />
      <MiniMap />
    </SvelteFlow>
  </div>
</div>
