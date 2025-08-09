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
  } from "@xyflow/svelte";

  import ProjectGraph from "./ProjectGraph.svelte";

  import { Textarea } from "flowbite-svelte";
  import "@xyflow/svelte/dist/style.css";
  import SvelteSelect from "svelte-select";
  import CustomNode from "./CustomNode.svelte";
  import { Drawer, Button, CloseButton, Label, Input } from "flowbite-svelte";
  import { ChevronRightOutline } from "flowbite-svelte-icons";
  import {
    Dropdown,
    DropdownItem,
    DropdownDivider,
    Navbar,
    Radio,
    NavUl,
    NavLi,
  } from "flowbite-svelte";
  import { ChevronDownOutline } from "flowbite-svelte-icons";
  import ContextMenu from "./ContextMenu.svelte";

  //  --- Variables and state definitions ---
  let nodes = $state<Node[]>([]);
  let edges = $state.raw<Edge[]>([]);

  // --- Nodes used for the project graph ---
  let projectNodes = $state<Node[]>([]);
  let projectEdges = $state.raw<Edge[]>([]);

  let selectedPipelineDropdown = $state(null);
  let selectedProjectDropdown = $state(null);
  let clientWidth: number = $state();
  let clientHeight: number = $state();

  const BACKEND_HOST = import.meta.env.VITE_BACKEND_HOST || "localhost";
  const BACKEND_PORT = import.meta.env.VITE_BACKEND_PORT || "8000";
  const BACKEND_URL = `http://${BACKEND_HOST}:${BACKEND_PORT}`;

  let selectedPipelineTarget = $state(null);
  let selectedProjectTarget = $state(null);
  let selectedMergeDropdown = $state<string[]>([]);

  let currentPipelineId = $state("");
  let currentProjectId = $state("");
  let currentTargetPipelineId = $state("");
  const nodeWidth = 172;
  const nodeHeight = 36;
  let nodeDrawereForm = $state({
    id: "",
    tag: "",
    notes: "",
    folder_path: "",
    node_parameters_yaml: "",
  });

  let pipelineDrawerForm = $state({
    id: "",
    tag: "",
    project_id: "",
    notes: "",
  });

  let projectDrawerForm = $state({
    id: "",
    tag: "",
    notes: "",
  });

  let menu: {
    id: string;
    top?: number;
    left?: number;
  } | null = $state(null);

  let isHiddenPipelinePanel = $state(true);
  let isHiddenNodePanel = $state(true);
  let isHiddenProjectPanel = $state(true);
  let ids_tags_dict_pipelines = $state<Record<string, string>>({});
  let ids_tags_dict_projects = $state<Record<string, string>>({});
  let pipelines_dropdown = $state<string[]>([]);
  let projects_dropdown = $state<string[]>([]);

  let nodeTypes = { custom: CustomNode };

  const dagreGraph = new dagre.graphlib.Graph();

  let radiostate_pipeline = $state(2); // selector for what to display in pipeline list 1 for ids, 2 for tags
  let radiostate_projects = $state(2); // selector for what to display in projects list 1 for ids, 2 for tags

  // --  Definitions of functions ---
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  // Define functions

  const handleContextMenu: NodeEventWithPointer = ({ event, node }) => {
    event.preventDefault();

    // Optionally, update clientWidth/clientHeight here if window size can change
    // clientWidth = window.innerWidth;
    // clientHeight = window.innerHeight;

    menu = {
      id: node.id,
      top: event.clientY < clientHeight - 200 ? event.clientY : undefined,
      left: event.clientX < clientWidth - 200 ? event.clientX : undefined,
    };
  };
  // Close the context menu if it's open whenever the window is clicked.
  function handlePaneClick() {
    menu = null;
  }

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

  async function copySelectedNodeFolderPathToClipboard() {
    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      alert("No node selected");
      return;
    }
    const folderPath = selectedNode.data?.folder_path;
    if (!folderPath) {
      alert("No folder path available for the selected node.");
      return;
    }
    try {
      await navigator.clipboard.writeText(folderPath);
      alert(`Folder path copied to clipboard:\n${folderPath}`);
    } catch (error) {
      console.error("Failed to copy folder path:", error);
      alert("Failed to copy folder path.");
    }
  }

  async function refreshLayout() {
    const layouted = getLayoutedElements(nodes, edges);
    nodes = [...layouted.nodes];
    edges = [...layouted.edges];

    // Save the new node positions to the backend
    await saveNodePositions();
  }

  async function saveNodePositions() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/update_node_position/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            nodes: nodes.map((n) => ({ id: n.id, position: n.position })),
          }),
        },
      );

      if (!response.ok) await handleApiError(response);
    } catch (error) {
      console.error("Error saving node positions:", error);
    }
  }

  async function addNode() {
    try {
      const pipelineId =
        typeof currentPipelineId === "string"
          ? currentPipelineId
          : currentPipelineId.value;

      if (!pipelineId) {
        console.error("No pipeline selected");
        return;
      }

      const response = await fetch(
        `${BACKEND_URL}/create_node_in_pipeline/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pipeline_id: pipelineId }),
        },
      );

      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error adding node:", error);
    }
  }

  async function deleteNode() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    if (!selectedNodeIds.length) {
      alert("Please select at least one node to delete.");
      return;
    }

    const confirmed = confirm(
      `Are you sure you want to delete the selected node(s): ${selectedNodeIds.join(", ")}? This action cannot be undone.`,
    );
    if (!confirmed) return;

    await Promise.all(
      selectedNodeIds.map(async (nodeId) => {
        try {
          const response = await fetch(
            `${BACKEND_URL}/delete_node_from_pipeline/${pipelineId}/${nodeId}`,
            {
              method: "DELETE",
              headers: { "Content-Type": "application/json" },
            },
          );

          if (!response.ok) await handleApiError(response);
        } catch (error) {
          console.error(`Error deleting node ${nodeId}:`, error);
        }
      }),
    );
    await loadPipeline(pipelineId);
  }

  async function deleteEdge() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
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
            `${BACKEND_URL}/delete_edge/${pipelineId}/${edge.source}/${edge.target}`,
            {
              method: "DELETE",
              headers: { "Content-Type": "application/json" },
            },
          );
          if (!response.ok) await handleApiError(response);
        } catch (error) {
          console.error(`Error deleting edge ${edgeId}:`, error);
        }
      }),
    );
    await loadPipeline(pipelineId);
  }

  async function duplicateSelectedNodes(withdata: boolean) {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    // Collect all selected node IDs
    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    if (!selectedNodeIds.length) {
      console.error("No nodes selected");
      alert("Please select at least one node to duplicate.");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/duplicate_nodes_in_pipeline`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_pipeline_id: pipelineId,
            target_pipeline_id: pipelineId,
            node_ids: selectedNodeIds,
            withdata: withdata, // Pass the withdata flag
          }),
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
      alert(`Nodes ${selectedNodeIds.join(", ")} duplicated successfully.`);
    } catch (error) {
      console.error("Error duplicating nodes:", error);
      alert("Failed to duplicate nodes.");
    }
  }

  async function branchPipelineWithParents(withdata: boolean) {

    const selectedNode = projectNodes.find((node) => node.selected);
    if (!selectedNode) {
      console.error("No node selected");
      return;
    }

    const projectId =
      typeof currentProjectId === "string"
        ? currentProjectId
        : currentProjectId?.value;

    if (!projectId) {
      alert("No project selected");
      return;
    }

    const selectedCount = projectNodes.filter((node) => node.selected).length;
    if (selectedCount !== 1) {
      alert("Please select exactly one pipeline node to load.");
      return;
    }

    if (!selectedNode.id) {
      console.error("Selected node has no ID");
      return;
    }

    const pipelineId = selectedNode.id;
    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/branch_pipeline_with_parents/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ withdata }),
        }
      );

      if (!response.ok) await handleApiError(response);

      const data = await response.json();
      alert(
        `Pipeline ${pipelineId} branched into new pipeline ${data.new_pipeline_id} with parent relationships maintained.`
      );

      await fetchPipelines();
      await fetchProjects();
      await loadProject(projectId);
      await loadPipeline(data.new_pipeline_id);
    } catch (error) {
      console.error("Error branching pipeline with parents:", error);
      alert("Failed to branch pipeline with parents.");
    }
  }

  async function duplicateSelectedNodesIntoPipeline(withdata: boolean) {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    if (!currentTargetPipelineId) {
      console.error("No target pipeline selected");
      alert("Please select a target pipeline to duplicate nodes into.");
      return;
    }

    // Collect all selected node IDs
    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    if (!selectedNodeIds.length) {
      console.error("No nodes selected");
      alert("Please select at least one node to duplicate.");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/duplicate_nodes_in_pipeline`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_pipeline_id: pipelineId,
            target_pipeline_id: currentTargetPipelineId,
            node_ids: selectedNodeIds,
            withdata: withdata, // Pass the withdata flag
          }),
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(currentTargetPipelineId);
      alert(`Nodes ${selectedNodeIds.join(", ")} duplicated successfully.`);
      selectedPipelineTarget = null;
    } catch (error) {
      console.error("Error duplicating nodes:", error);
      alert("Failed to duplicate nodes.");
    }
  }

  async function detachSelectedNodeFromPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      alert("No pipeline selected");
      return;
    }

    const selectedNode = nodes.find((node) => node.selected);

    if (!selectedNode) {
      alert("No node selected");
      return;
    }

    const nodeId = selectedNode.id;

    try {
      const response = await fetch(
        `${BACKEND_URL}/detach_subgraph_from_node/${pipelineId}/${nodeId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
      alert(
        `Node ${nodeId} detached from pipeline ${pipelineId} as a new node.`
      );
    } catch (error) {
      console.error("Error detaching node from pipeline:", error);
      alert("Failed to detach node from pipeline.");
    }
  }

  async function referenceSelectedNodesIntoPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    if (!currentTargetPipelineId) {
      console.error("No target pipeline selected");
      alert("Please select a target pipeline to duplicate nodes into.");
      return;
    }

    // Collect all selected node IDs
    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    if (!selectedNodeIds.length) {
      console.error("No nodes selected");
      alert("Please select at least one node to duplicate.");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/reference_nodes_into_pipeline`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            source_pipeline_id: pipelineId,
            target_pipeline_id: currentTargetPipelineId,
            node_ids: selectedNodeIds,
          }),
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(currentTargetPipelineId);
      alert(`Nodes ${selectedNodeIds.join(", ")} referenced successfully from pipeline ${pipelineId} into pipeline ${currentTargetPipelineId}.`);
      currentTargetPipelineId = "";
    } catch (error) {
      console.error("Error duplicating nodes:", error);
      alert("Failed to duplicate nodes.");
    }
  }

  async function moveSelectedPipelinetoProject() {
    if (!selectedProjectTarget) {
      console.error("No target project selected");
      alert("Please select a target project to move the pipeline into.");
      return;
    }

    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }
    try {
      let projectId;
      if (radiostate_projects === 1) {
        // Dropdown is showing IDs, so use directly
        projectId = selectedProjectTarget.value;
      } else if (radiostate_projects === 2) {
        // Dropdown is showing tags, so lookup ID from dict
        projectId = Object.keys(ids_tags_dict_projects).find(
          (key) => ids_tags_dict_projects[key] === selectedProjectTarget.value,
        );
      }
      if (!projectId) {
        alert("Could not determine project ID to move the pipeline into.");
        return;
      }

      const response = await fetch(`${BACKEND_URL}/move_pipeline_to_project`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          project_id: projectId,
          pipeline_id: pipelineId,
        }),
      });
      if (!response.ok) await handleApiError(response);
      await fetchPipelines();
      currentPipelineId = "";
      nodes = [];
      edges = [];
      alert(
        `Pipeline ${pipelineId} moved to project ${projectId} successfully.`,
      );
      selectedProjectTarget = null;
    } catch (error) {
      console.error("Error moving pipeline to project:", error);
      alert("Failed to move pipeline to project.");
    }
  }

  async function setNodeCompleted() {
    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      alert("No node selected");
      return;
    }
    try {
      const pipelineId =
        typeof currentPipelineId === "string"
          ? currentPipelineId
          : currentPipelineId.value;
      const response = await fetch(
        `${BACKEND_URL}/manual_set_node_status/${selectedNode.id}/completed`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pipeline_id: pipelineId }),
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
      alert(`Node ${selectedNode.id} status set to completed.`);
    } catch (error) {
      console.error("Error setting node status:", error);
      alert("Failed to set node status.");
    }
  }

  async function setNodeStaleData() {
    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      alert("No node selected");
      return;
    }
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;
    try {
      const response = await fetch(
        `${BACKEND_URL}/manual_set_node_status/${selectedNode.id}/staledata`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pipeline_id: pipelineId }),
        },
      );
      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
      alert(`Node ${selectedNode.id} status set to completed.`);
    } catch (error) {
      console.error("Error setting node status:", error);
      alert("Failed to set node status.");
    }
  }

  async function fetchPipelines() {
    try {
      const response = await fetch(
        `${BACKEND_URL}/get_all_pipeline_ids_tags_dict`,
      );
      if (!response.ok) await handleApiError(response);
      ids_tags_dict_pipelines = await response.json();
    } catch (error) {
      console.error("Error fetching pipelines:", error);
    }
  }

  async function fetchProjects() {
    try {
      const response = await fetch(
        `${BACKEND_URL}/get_all_project_ids_tags_dict`,
      );
      if (!response.ok) await handleApiError(response);
      // Assuming the API returns a similar dict as pipelines
      ids_tags_dict_projects = await response.json();
    } catch (error) {
      console.error("Error fetching projects:", error);
    }
  }

  async function handleConnect(event) {
    const source = event.source;
    const target = event.target;

    console.log(`Connected nodes: ${source} → ${target}`);

    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(`${BACKEND_URL}/connect_nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source, target, pipelineId }),
      });
      if (!response.ok) {
        await handleApiError(response);
      }
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error connecting nodes:", error);
      await loadPipeline(pipelineId);
      return false;
    }
  }

  async function createPipeline() {
    const projectId =
      typeof currentProjectId === "string"
        ? currentProjectId
        : currentProjectId?.value;

    if (!projectId) {
      alert("Please select a project before creating a pipeline.");
      return;
    }

    try {
      const response = await fetch(`${BACKEND_URL}/create_pipeline`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ project_id: projectId }),
      });
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      const newPipelineId = data.pipeline_id || data.id || data.pip_id;
      await fetchPipelines();
      currentPipelineId = newPipelineId;
      await loadProject(projectId);
      await loadPipeline(newPipelineId);
    } catch (error) {
      console.error("Error creating pipeline:", error);
    }
  }

  async function createProject() {
    try {
      const response = await fetch(`${BACKEND_URL}/create_project`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      const newProjectId = data.project_id;
      currentProjectId = newProjectId;
      await fetchProjects();
      await loadProject(newProjectId);
      clearPipelineVariables();
    } catch (error) {
      console.error("Error creating project:", error);
    }
  }

  async function deleteSelectedPipeline() {
    if (!currentPipelineId) {
      alert("No pipeline selected");
      return;
    }

    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;
    const confirmed = confirm(
      `Are you sure you want to delete pipeline "${pipelineId}"? This action cannot be undone.`,
    );

    if (!confirmed) return;

    try {
      const response = await fetch(
        `${BACKEND_URL}/delete_pipeline/${pipelineId}`,
        {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
        },
      );
      if (!response.ok) await handleApiError(response);
      await fetchPipelines();
      clearPipelineVariables();
      await loadProject(currentProjectId);
    } catch (error) {
      console.error("Error deleting pipeline:", error);
      alert("Failed to delete pipeline.");
    }
  }
  async function deleteCurrentProject() {
    if (!currentProjectId) {
      alert("No project selected");
      return;
    }

    // Ask the user to type the project id to confirm deletion
    const userInput = prompt(
      `To confirm deletion, please type the project id exactly:\n${currentProjectId}`
    );
    if (userInput === null) return; // User cancelled

    if (userInput !== currentProjectId) {
      alert("Project id does not match. Deletion cancelled.");
      return;
    }

    const confirmed = confirm(
      `Are you sure you want to delete project "${currentProjectId}"? This action cannot be undone.`
    );
    if (!confirmed) return;

    try {
      const response = await fetch(
        `${BACKEND_URL}/delete_project/${currentProjectId}`,
        {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
        }
      );
      if (!response.ok) await handleApiError(response);
      await fetchProjects();
      currentProjectId = "";
      alert("Project deleted successfully.");
      clearProjectVariables();
    } catch (error) {
      console.error("Error deleting project:", error);
      alert("Failed to delete project.");
    }
  }

  async function branchPipelineFromNode() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
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
        `${BACKEND_URL}/branch_pipeline/${pipelineId}/${startNodeId}`,
        {
          method: "GET",
          headers: { "Content-Type": "application/json" },
        },
      );
      if (!response.ok) await handleApiError(response);
      const data = await response.json();

      console.log("Pipeline iteration result:", data);
      await loadProject(currentProjectId);
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
        `${BACKEND_URL}/get_pipeline/${pipelineId}`,
        { cache: "no-store" },
      );
      if (!response.ok) await handleApiError(response);
      const pipeline = await response.json();

      const rawNodes = Object.entries(pipeline.nodes).map(([id, node]) => ({
        id,
        type: "custom",
        data: {
          line1: `TAG: ${node.tag}`,
          line2: `ID: ${id}`,
          referenced: node.referenced,
          notes: node.notes || "",
          folder_path: node.folder_path || "",
          status: node.status || "ready",
          tag: node.tag || "",
          blocked: node.blocked || false,
        },
        position: {
          x: node.position[0],
          y: node.position[1],
        },
        style: `background: ${getNodeColor(node.status)}`,
      }));

      const rawEdges = Object.entries(pipeline.nodes).flatMap(([id, node]) =>
        node.parents.map((parentId) => ({
          id: `${parentId}-${id}`,
          source: parentId,
          target: id,
          blocked: node.blocked || false,
        })),
      );

      // Only apply layout if there are no positions stored
      const needsLayout = rawNodes.some((node) => !node.position);

      if (needsLayout) {
        const layoutedElements = getLayoutedElements(rawNodes, rawEdges);
        nodes = [...layoutedElements.nodes];
        edges = [...layoutedElements.edges];
      } else {
        nodes = [...rawNodes];
        edges = [...rawEdges];
      }

      currentPipelineId = pipelineId;
      currentProjectId = pipeline.project_id || "";
    
      // Set status property to "active" for the projectNode that matches the pipeline id,
      // and set the background color accordingly
      projectNodes = projectNodes.map((node) => {
        if (node.id === pipelineId) {
          return {
            ...node,
            data: {
              ...node.data,
            },
            style: `background: ${getNodeColor("completed")}`,
          };
        } else {
          return {
            ...node,
            data: {
              ...node.data,
            },
            style: `background: ${getNodeColor("")}`,
          };
        }
      });
    } catch (error) {
      console.error("Error loading selected pipeline:", error);
    }
  }

  // Load a project
  async function loadProject(project_id: string) {
    try {
      const response = await fetch(`${BACKEND_URL}/get_project/${project_id}`, {
        cache: "no-store",
      });
      if (!response.ok) await handleApiError(response);
      const project = await response.json();

      const rawNodes = Object.entries(project.nodes).map(([id, node]) => ({
        id,
        type: "custom",
        data: {
          line1: `TAG: ${node.tag}`,
          line2: `ID: ${id}`,
          notes: node.notes || "",
          tag: node.tag || "",
          blocked: node.blocked,
        },
      }));

      const rawEdges = Object.entries(project.nodes).flatMap(([id, node]) =>
        node.parents.map((parentId) => ({
          id: `${parentId}-${id}`,
          source: parentId,
          target: id,
        })),
      );

      const layoutedElements = getLayoutedElements(rawNodes, rawEdges);
      projectNodes = [...layoutedElements.nodes];
      projectEdges = [...layoutedElements.edges];

      currentProjectId = project.project_id || "";
    } catch (error) {
      console.error("Error loading selected pipeline:", error);
    }
  }

  // Block all nodes in the selected pipeline
  async function blockAllNodesInPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
      ? currentPipelineId
      : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/block_all_nodes_in_pipeline/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        }
      );

      if (!response.ok) await handleApiError(response);

      alert(`All nodes in pipeline ${pipelineId} blocked successfully.`);
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error blocking all nodes in pipeline:", error);
      alert("Failed to block all nodes in pipeline.");
    }
  }

  // Unblock all nodes in the selected pipeline  
  async function unblockAllNodesInPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
      ? currentPipelineId
      : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(
        `${BACKEND_URL}/unblock_all_nodes_in_pipeline/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
        }
      );

      if (!response.ok) await handleApiError(response);

      alert(`All nodes in pipeline ${pipelineId} unblocked successfully.`);
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error unblocking all nodes in pipeline:", error);
      alert("Failed to unblock all nodes in pipeline.");
    }
  }

  // Load the selected pipeline when the component mounts or when currentPipelineId changes
  async function loadSelectedDropdownPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!currentPipelineId) {
      console.error("No pipeline selected");
      return;
    }

    await loadPipeline(pipelineId);
  }

  // Load the selected pipeline when the component mounts or when currentPipelineId changes
  async function loadSelectedGraphPipeline() {
    const selectedNode = projectNodes.find((node) => node.selected);
    if (!selectedNode) {
      console.error("No node selected");
      return;
    }

    const selectedCount = projectNodes.filter((node) => node.selected).length;
    if (selectedCount !== 1) {
      alert("Please select exactly one pipeline node to load.");
      return;
    }

    if (!selectedNode.id) {
      console.error("Selected node has no ID");
      return;
    }

    const pipelineId = selectedNode.id;
    selectedProjectDropdown = null;

    await loadPipeline(pipelineId);
  }

  // Load the selected project when the component mounts or when currentProjectId changes
  async function loadSelectedProject() {
    const projectId =
      typeof currentProjectId === "string"
        ? currentProjectId
        : currentProjectId.value;

    if (!currentProjectId) {
      console.error("No project selected");
      return;
    }

    selectedProjectDropdown = null;
    currentProjectId = projectId;
    await loadProject(projectId);
    await clearPipelineVariables();
  }

  // Fetch node_parameters for a node
  async function fetchNodeParametersYaml(nodeId: string) {
    if (!nodeId) {
      nodeDrawereForm.node_parameters_yaml = "";
      return;
    }
    try {
      const response = await fetch(
        `${BACKEND_URL}/get_node_parameters_yaml/${nodeId}`,
      );
      if (!response.ok) {
        nodeDrawereForm.node_parameters_yaml = "";
        return;
      }
      const data = await response.json();
      nodeDrawereForm.node_parameters_yaml = data.content || "";
    } catch (error) {
      nodeDrawereForm.node_parameters_yaml = "";
    }
  }

  // Save node_parameters for a node
  async function saveNodeParametersYaml(nodeId: string) {
    try {
      const response = await fetch(
        `${BACKEND_URL}/update_node_parameters_yaml/${nodeId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            content: nodeDrawereForm.node_parameters_yaml,
          }),
        },
      );
      if (!response.ok) await handleApiError(response);
      alert("Node parameters saved.");
    } catch (error) {
      alert("Failed to save node parameters.");
    }
  }

  // Add refs for the input fields

  async function updateNodeInfo() {
    if (!nodeDrawereForm || !currentPipelineId) {
      console.error("No node or pipeline selected");
      return;
    }

    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;
    const nodeId = nodeDrawereForm.id;
    const newTag = nodeDrawereForm.tag;
    const newNotes = nodeDrawereForm.notes;
    const newFolderPath = nodeDrawereForm.folder_path;

    try {
      // Update node tag
      await fetch(`${BACKEND_URL}/update_node_tag/${pipelineId}/${nodeId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_tag: newTag }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      // Update node notes
      await fetch(`${BACKEND_URL}/update_node_notes/${pipelineId}/${nodeId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notes: newNotes }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      await saveNodeParametersYaml(nodeId); // Save YAML content
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
        `${BACKEND_URL}/get_pipeline/${pipelineId}`,
        { cache: "no-store" },
      );
      if (!response.ok) await handleApiError(response);
      const pipeline = await response.json();
      pipelineDrawerForm = {
        id: pipeline.pipeline_id || "",
        tag: pipeline.tag || "",
        notes: pipeline.notes || "",
        project_id: pipeline.project_id || "",
      };
    } catch (error) {
      console.error("Error loading pipeline info:", error);
      pipelineDrawerForm = { id: "", tag: "", notes: "", project_id: "" };
    }
  }

  async function loadProjectInfo(projectId: string) {
    try {
      const response = await fetch(`${BACKEND_URL}/get_project/${projectId}`, {
        cache: "no-store",
      });
      if (!response.ok) await handleApiError(response);
      const project = await response.json();
      projectDrawerForm = {
        id: project.project_id || "",
        tag: project.tag || "",
        notes: project.notes || "",
      };
    } catch (error) {
      console.error("Error loading project info:", error);
      projectDrawerForm = { id: "", tag: "", notes: "" };
    }
  }

  async function filterPipelinesByProject() {
    let projectId;
    if (radiostate_projects === 1) {
      projectId = selectedProjectDropdown.value;
    } else if (radiostate_projects === 2) {
      // Dropdown contains tags, so find the project ID for the selected tag
      projectId = Object.keys(ids_tags_dict_projects).find(
        (key) => ids_tags_dict_projects[key] === selectedProjectDropdown.value,
      );
    }
    if (!projectId) {
      pipelines_dropdown = [];
      return;
    }
    try {
      const response = await fetch(
        `${BACKEND_URL}/get_pipelines_in_project/${projectId}`,
      );
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      if (data.pipelines) {
        // Filter ids_tags_dict_pipelines to only include these pipeline ids
        pipelines_dropdown = data.pipelines
          .map((id) =>
            radiostate_pipeline === 1 ? id : ids_tags_dict_pipelines[id],
          )
          .filter(Boolean);
      } else {
        pipelines_dropdown = [];
      }
    } catch (error) {
      console.error("Error filtering pipelines by project:", error);
      pipelines_dropdown = [];
    }
  }

  // Add a function to update pipeline info
  async function updatePipelineInfo() {
    if (!pipelineDrawerForm || !currentPipelineId) {
      console.error("No pipeline selected");
      return;
    }
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;
    const newTag = pipelineDrawerForm.tag;
    const newNotes = pipelineDrawerForm.notes;

    try {
      await fetch(`${BACKEND_URL}/update_pipeline_tag/${pipelineId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tag: newTag }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      await fetch(`${BACKEND_URL}/update_pipeline_notes/${pipelineId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notes: newNotes }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      await fetchPipelines();
      await loadPipeline(pipelineId);
      alert("Pipeline info updated.");
    } catch (error) {
      console.error("Error updating pipeline info:", error);
      alert("Failed to update pipeline info.");
    }
  }

  // Add a function to update project info
  async function updateProjectInfo() {
    if (!projectDrawerForm || !currentProjectId) {
      console.error("No project selected");
      return;
    }
    const projectId =
      typeof currentProjectId === "string"
        ? currentProjectId
        : currentProjectId.value;
    const newTag = projectDrawerForm.tag;
    const newNotes = projectDrawerForm.notes;

    try {
      await fetch(`${BACKEND_URL}/update_project_tag/${projectId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tag: newTag }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      await fetch(`${BACKEND_URL}/update_project_notes/${projectId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ notes: newNotes }),
      }).then(async (response) => {
        if (!response.ok) await handleApiError(response);
      });
      await fetchProjects();
      alert("Project info updated.");
    } catch (error) {
      console.error("Error updating project info:", error);
      alert("Failed to update project info.");
    }
  }

  async function runSelectedNode() {
    const selectedNode = nodes.find((node) => node.selected);
    const pipelineatcall =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;
    if (!selectedNode) {
      alert("No node selected");
      return;
    }
    const nodeId = selectedNode.id;
    alert(`Node ${nodeId} is starting to run...`);
    try {
      const response = await fetch(`${BACKEND_URL}/run_node/${nodeId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_mode: "local" }),
      });
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      // Optionally reload pipeline to update node status
      if (currentPipelineId && pipelineatcall === currentPipelineId) {
        const pipelineId =
          typeof currentPipelineId === "string"
            ? currentPipelineId
            : currentPipelineId.value;
        await loadPipeline(pipelineId);
      }
    } catch (error) {
      console.error("Error running node:", error);
      alert("Failed to run node.");
    }
  }

  async function runCurrentPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      alert("No pipeline selected");
      return;
    }

    // Notify user
    alert(`Pipeline ${pipelineId} is starting to run...`);

    try {
      const response = await fetch(
        `${BACKEND_URL}/run_pipeline/${pipelineId}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ run_mode: "local" }),
        },
      );
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      await loadPipeline(pipelineId);
      alert(data.message || "Pipeline run completed.");
    } catch (error) {
      console.error("Error running pipeline:", error);
      alert("Failed to run pipeline.");
    }
  }

  async function runPipelineUpToNode() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      alert("No pipeline selected");
      return;
    }

    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      alert("No node selected");
      return;
    }

    // Notify user
    alert(`Pipeline ${pipelineId} will run up to node ${selectedNode.id}...`);

    try {
      const response = await fetch(
        `${BACKEND_URL}/run_pipeline_up_to_node/${pipelineId}/${selectedNode.id}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ run_mode: "local" }),
        },
      );
      if (!response.ok) await handleApiError(response);
      const data = await response.json();
      await loadPipeline(pipelineId);
      alert(data.message || "Pipeline run completed.");
    } catch (error) {
      console.error("Error running pipeline:", error);
      alert("Failed to run pipeline.");
    }
  }

  async function deleteNodeOutputs() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    const selectedNodeIds = nodes
      .filter((node) => node.selected)
      .map((node) => node.id);

    if (!selectedNodeIds.length) {
      alert("Please select at least one node to delete outputs.");
      return;
    }

    const confirmed = confirm(
      `Are you sure you want to delete outputs for nodes: ${selectedNodeIds.join(", ")}? This action cannot be undone.`,
    );
    if (!confirmed) return;

    try {
      const response = await fetch(`${BACKEND_URL}/delete_node_data/`, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          node_ids: selectedNodeIds,
          pipeline_id: pipelineId,
        }),
      });
      if (!response.ok) await handleApiError(response);
      await loadPipeline(pipelineId);
      alert(
        `Outputs for nodes ${selectedNodeIds.join(", ")} deleted successfully.`,
      );
    } catch (error) {
      console.error("Error deleting node outputs:", error);
      alert("Failed to delete node outputs.");
    }
  }

  async function blockSelectedNodes() {
    const selectedNodeIds = nodes.filter((node) => node.selected).map((node) => node.id);
    if (!selectedNodeIds.length) {
      alert("Please select at least one node to block.");
      return;
    }
    try {
      const response = await fetch(`${BACKEND_URL}/block_nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_ids: selectedNodeIds }),
      });
      if (!response.ok) await handleApiError(response);
      alert(`Nodes ${selectedNodeIds.join(", ")} blocked successfully.`);
      await loadPipeline(currentPipelineId);
    } catch (error) {
      console.error("Error blocking nodes:", error);
      alert("Failed to block nodes.");
    }
  }

  async function unblockSelectedNodes() {
    const selectedNodeIds = nodes.filter((node) => node.selected).map((node) => node.id);
    if (!selectedNodeIds.length) {
      alert("Please select at least one node to unblock.");
      return;
    }
    try {
      const response = await fetch(`${BACKEND_URL}/unblock_nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_ids: selectedNodeIds }),
      });
      if (!response.ok) await handleApiError(response);
      alert(`Nodes ${selectedNodeIds.join(", ")} unblocked successfully.`);
      await loadPipeline(currentPipelineId);
    } catch (error) {
      console.error("Error unblocking nodes:", error);
      alert("Failed to unblock nodes.");
    }
  }

  // Helper function for API error handling
  async function handleApiError(response: Response) {
    let errorMsg = response.statusText;
    try {
      const errorData = await response.json();
      errorMsg = errorData.detail || errorMsg;
    } catch (e) {
      // fallback if not JSON
    }
    if (typeof errorMsg !== "string") {
      errorMsg = JSON.stringify(errorMsg, null, 2);
    }
    alert("Error: " + errorMsg);
    throw new Error(errorMsg);
  }

  async function killSelectedNode() {
    const selectedNode = nodes.find((node) => node.selected);
    if (!selectedNode) {
      alert("No node selected");
      return;
    }
    const nodeId = selectedNode.id;
    try {
      const response = await fetch(`${BACKEND_URL}/kill_node/${nodeId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      if (!response.ok) await handleApiError(response);
      alert(`Kill signal sent to node ${nodeId}.`);
      // Optionally reload pipeline to update node status
      if (currentPipelineId) {
        const pipelineId =
          typeof currentPipelineId === "string"
            ? currentPipelineId
            : currentPipelineId.value;
        await loadPipeline(pipelineId);
      }
    } catch (error) {
      console.error("Error killing node:", error);
      alert("Failed to kill node.");
    }
  }


  async function blockCurrentPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(`${BACKEND_URL}/block_pipeline/${pipelineId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      if (!response.ok) {
        await handleApiError(response);
      }

      alert(`Pipeline ${pipelineId} and all its nodes blocked successfully.`);
      await loadProject(currentProjectId);
      await loadPipeline(pipelineId);

    } catch (error) {
      console.error("Error blocking pipeline:", error);
      alert("Failed to block pipeline.");
    }
  }


  async function unblockCurrentPipeline() {
    const pipelineId =
      typeof currentPipelineId === "string"
        ? currentPipelineId
        : currentPipelineId.value;

    if (!pipelineId) {
      console.error("No pipeline selected");
      return;
    }

    try {
      const response = await fetch(`${BACKEND_URL}/unblock_pipeline/${pipelineId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });

      if (!response.ok) {
        await handleApiError(response);
      }

      alert(`Pipeline ${pipelineId} and all its nodes unblocked successfully.`);
      await loadProject(currentProjectId);
      await loadPipeline(pipelineId);
    } catch (error) {
      console.error("Error unblocking pipeline:", error);
      alert("Failed to unblock pipeline.");
    }
  }


  function getCurrentPipelineIdsMerge(
    selectedMergeDropdown: string[],
  ): string[] {
    if (!selectedMergeDropdown || selectedMergeDropdown.length === 0) {
      return [];
    }

    if (radiostate_pipeline === 1) {
      return selectedMergeDropdown.map((item) => item.value);
    } else if (radiostate_pipeline === 2) {
      return selectedMergeDropdown
        .map((tag) =>
          Object.keys(ids_tags_dict_pipelines).find(
            (key) => ids_tags_dict_pipelines[key] === tag.value,
          ),
        )
        .filter(Boolean) as string[];
    }

    return [];
  }

  async function mergePipeline() {
    const pipelineIdsToMerge = getCurrentPipelineIdsMerge(
      selectedMergeDropdown,
    );

    if (!pipelineIdsToMerge || pipelineIdsToMerge.length < 2) {
      alert("Please select at least two pipelines to merge.");
      return;
    }

    try {
      const response = await fetch(`${BACKEND_URL}/merge_pipelines`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source_pipeline_ids: pipelineIdsToMerge }),
      });

      if (!response.ok) {
        await handleApiError(response);
      }

      const data = await response.json();
      alert(
        `Pipelines ${pipelineIdsToMerge.join(", ")} merged into new pipeline ${
          data.target_pipeline_id
        }.`,
      );

      await fetchPipelines();
      currentPipelineId = data.target_pipeline_id;
      await loadPipeline(data.target_pipeline_id);
      selectedMergeDropdown = [];
    } catch (error) {
      console.error("Error merging pipelines:", error);
      alert("Failed to merge pipelines.");
      selectedMergeDropdown = [];
    }
  }

  async function clearPipelineVariables() {
    nodes = [];
    edges = [];
    currentPipelineId = "";
  }

  async function clearProjectVariables() {
    projectNodes = [];
    projectEdges = [];
    currentProjectId = "";
  }

  // ------------ Collection of all reactive effects ---------------
  // Effect to update nodeDrawereForm when a node is selected
  $effect(() => {
    if (!isHiddenNodePanel) {
      const selectedNodes = nodes.filter((node) => node.selected);
      if (selectedNodes.length === 1) {
        const selectedNode = selectedNodes[0];
        nodeDrawereForm = {
          id: selectedNode.id,
          tag: selectedNode.data?.tag || "No tag",
          notes: selectedNode.data?.notes || "",
          folder_path: selectedNode.data?.folder_path || "",
        };
        fetchNodeParametersYaml(selectedNode.id);
      } else {
        nodeDrawereForm = {
          id: "",
          tag: "",
          notes: "",
          node_parameters_yaml: "",
        };
        nodeParametersYaml = "";
      }
    } else {
      nodeDrawereForm = {
        id: "",
        tag: "",
        notes: "",
        node_parameters_yaml: "",
      };
    }
  });

  // Effect to update currentProjectId when a project is selected
  $effect(() => {
    if (!isHiddenProjectPanel && currentProjectId) {
      let projectId =
        typeof currentProjectId === "string"
          ? currentProjectId
          : currentProjectId.value;
      loadProjectInfo(projectId);
    }
  });

  // Effect to load pipeline info when the panel is opened
  $effect(() => {
    if (!isHiddenPipelinePanel && currentPipelineId) {
      const pipelineId =
        typeof currentPipelineId === "string"
          ? currentPipelineId
          : currentPipelineId.value;
      loadPipelineInfo(pipelineId);
    }
    if (isHiddenPipelinePanel) {
      pipelineDrawerForm = { id: "", tag: "", notes: "", project_id: "" };
    }
  });

  // Effect to update pipelineDrawerForm when a pipeline is selected
  $effect(() => {
    if (radiostate_pipeline === 1) {
      pipelines_dropdown = Object.keys(ids_tags_dict_pipelines);
    } else if (radiostate_pipeline === 2) {
      pipelines_dropdown = Object.values(ids_tags_dict_pipelines);
    }
  });

  // Effect to update projectDrawerForm when a pipeline is selected
  $effect(() => {
    if (radiostate_projects === 1) {
      projects_dropdown = Object.keys(ids_tags_dict_projects);
    } else if (radiostate_projects === 2) {
      projects_dropdown = Object.values(ids_tags_dict_projects);
    }
    selectedProjectDropdown = null;
  });

  // Effect to update currentPipelineId when a pipeline is selected
  $effect(() => {
    if (selectedPipelineDropdown) {
      if (radiostate_pipeline === 1) {
        currentPipelineId = selectedPipelineDropdown.value;
      } else if (radiostate_pipeline === 2) {
        // Dropdown contains tags, so find the pipeline ID for the selected tag
        currentPipelineId = Object.keys(ids_tags_dict_pipelines).find(
          (key) =>
            ids_tags_dict_pipelines[key] === selectedPipelineDropdown.value,
        );
      }
    }
  });

  // Effect to update currentTargetPipelineId when a target pipeline is selected
  $effect(() => {
    if (selectedPipelineTarget) {
      if (radiostate_pipeline === 1) {
        currentTargetPipelineId = selectedPipelineTarget.value;
      } else if (radiostate_pipeline === 2) {
        // Dropdown contains tags, so find the pipeline ID for the selected tag
        currentTargetPipelineId = Object.keys(ids_tags_dict_pipelines).find(
          (key) =>
            ids_tags_dict_pipelines[key] === selectedPipelineTarget.value,
        );
      }
    }
  });

  // Effect to update currentProjectId when a project is selected
  $effect(() => {
    if (selectedProjectDropdown) {
      if (radiostate_projects === 1) {
        currentProjectId = selectedProjectDropdown.value;
      } else if (radiostate_projects === 2) {
        // Dropdown contains tags, so find the project ID for the selected tag
        currentProjectId = Object.keys(ids_tags_dict_projects).find(
          (key) =>
            ids_tags_dict_projects[key] === selectedProjectDropdown.value,
        );
      }
    }
  });

  // Filter pipelines by project when a project is selected
  $effect(() => {
    if (selectedProjectDropdown) {
      filterPipelinesByProject();
    } else {
      // No project selected: show all pipelines
      if (radiostate_pipeline === 1) {
        pipelines_dropdown = Object.keys(ids_tags_dict_pipelines);
      } else if (radiostate_pipeline === 2) {
        pipelines_dropdown = Object.values(ids_tags_dict_pipelines);
      }
      currentProjectId = "";
    }
  });

  // Use effect to handle node drag end event and save positions
  $effect(() => {
    nodes.forEach((node) => {
      if (node.dragging === false && node.selected) {
        saveNodePositions();
      }
    });
  });

  // All effect at refresh
  $effect(fetchPipelines);
  $effect(fetchProjects);
</script>

<!-- All graphics -->
<div class="app-layout">
  <!-- Two Column Container -->
  <div class="two-column-wrapper">
    <!-- Left Column -->
    <div class="left-column">
      <Navbar>
        <NavUl class="ms-3 pt-1">
          <NavLi class="cursor-pointer">
            Project interaction<ChevronDownOutline
              class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
            />
          </NavLi>
          <Dropdown simple>
            <DropdownItem>
              <li>
                <Radio
                  name="radio_state"
                  bind:group={radiostate_projects}
                  value={1}>List ids</Radio
                >
              </li>
              <li>
                <Radio
                  name="radio_state"
                  bind:group={radiostate_projects}
                  value={2}>List tags</Radio
                >
              </li>
            </DropdownItem>
            <DropdownItem>
              <div class="w-64">
                <SvelteSelect
                  items={projects_dropdown}
                  bind:value={selectedProjectDropdown}
                  placeholder="Select a project..."
                  maxItems={5}
                  on:select={loadSelectedProject}
                />
              </div>
            </DropdownItem>

            <DropdownDivider />
            <DropdownItem onclick={() => (isHiddenProjectPanel = false)}
              >Open Active Project Panel</DropdownItem
            >
            <DropdownItem onclick={createProject}>Create New Project</DropdownItem>
            <DropdownItem class="text-red-600" onclick={deleteCurrentProject}
              >Delete Active Project</DropdownItem
            >
          </Dropdown>
          <NavLi class="cursor-pointer">
            Pipeline Interaction<ChevronDownOutline
              class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
            />
          </NavLi>
          <Dropdown simple>
            <DropdownItem>
              <li>
                <Radio
                  name="radio_state"
                  bind:group={radiostate_pipeline}
                  value={1}>List ids</Radio
                >
              </li>
              <li>
                <Radio
                  name="radio_state"
                  bind:group={radiostate_pipeline}
                  value={2}>List tags</Radio
                >
              </li>
            </DropdownItem>
            <DropdownItem>
              <div class="w-64">
                <SvelteSelect
                  items={pipelines_dropdown}
                  bind:value={selectedPipelineDropdown}
                  placeholder="Select a pipeline..."
                  maxItems={5}
                  on:select={loadSelectedDropdownPipeline}
                />
              </div>
            </DropdownItem>

            <DropdownDivider />
            <DropdownItem onclick={loadSelectedGraphPipeline}
              >Load selected Pipeline</DropdownItem
            >
            <DropdownItem onclick={() => (isHiddenPipelinePanel = false)}
              >Open selected pipeline panel</DropdownItem
            >
            <DropdownItem onclick={createPipeline}>Create new pipeline</DropdownItem
            >
            <DropdownItem onclick={blockCurrentPipeline}>Block active pipeline</DropdownItem
              >
            <DropdownItem onclick={unblockCurrentPipeline}>Unblock active pipeline</DropdownItem
              >              
            <DropdownItem class="flex items-center justify-between">
              Move Pipeline to project<ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>
            <Dropdown simple placement="right-start">
              <div class="w-64">
                <SvelteSelect
                  items={projects_dropdown}
                  bind:value={selectedProjectTarget}
                  placeholder="Select a project..."
                  maxItems={5}
                />
              </div>
              <Button onclick={moveSelectedPipelinetoProject} class="mt-2"
                >Move</Button
              >
            </Dropdown>

            <DropdownItem class="flex items-center justify-between">
              Duplicate pipeline into this project 
              <ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>
            <Dropdown simple placement="right-start">
              <Button onclick={() => branchPipelineWithParents(true)}>
                Duplicate with data
              </Button>
              <Button onclick={() => branchPipelineWithParents(false)}>
                Duplicate without data
              </Button>
            </Dropdown>
            <DropdownItem class="flex items-center justify-between">
              Merge pipelines<ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>

            <Dropdown simple placement="right-start">
              <div class="w-70">
                <SvelteSelect
                  items={pipelines_dropdown}
                  bind:value={selectedMergeDropdown}
                  placeholder="Select multiple pipelines..."
                  multiple={true}
                />
              </div>
              <Button class="mt-2" onclick={mergePipeline}>Merge</Button>
            </Dropdown>

            <DropdownItem class="text-red-600" onclick={deleteSelectedPipeline}
              >Delete Pipeline</DropdownItem
            >
          </Dropdown>
        </NavUl>
      </Navbar>

      <div class="main-content" bind:clientWidth bind:clientHeight>

        <!-- Svelte Flow restric only the varables node and edges to be used with 2 way bind. 
         In order to make it work for the second flow it was needed to create an independent component -->
        <ProjectGraph
          nodes={projectNodes}
          edges={projectEdges}
          {currentProjectId}
          {ids_tags_dict_projects}
          {nodeTypes}
          onUpdate={(e) => {
            projectNodes = e.detail.nodes;
            projectEdges = e.detail.edges;
          }}
        />
      </div>
    </div>

    <!-- Right Column - Current Content -->
    <div class="right-column">
      <Navbar>
        <NavUl class="ms-3 pt-1">
          <NavLi class="cursor-pointer">
            Node interaction<ChevronDownOutline
              class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
            />
          </NavLi>
          <Dropdown simple>
            <DropdownItem onclick={() => (isHiddenNodePanel = false)}
              >Open selected node panel</DropdownItem
            >
            <DropdownItem onclick={copySelectedNodeFolderPathToClipboard}>
              Copy selected node path to clipboard
            </DropdownItem>
            <DropdownItem onclick={addNode}>Create new node</DropdownItem>
            <DropdownItem onclick={branchPipelineFromNode}
              >Branch Pipeline from selected node</DropdownItem
            >
            <DropdownItem onclick={detachSelectedNodeFromPipeline}>Detach subtree from selected node</DropdownItem>            
            <DropdownItem class="flex items-center justify-between">
              Duplicate selected nodes into this pipeline 
              <ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>
            <Dropdown simple placement="right-start">
              <Button onclick={() => duplicateSelectedNodes(true)}>
                Duplicate with data
              </Button>
              <Button onclick={() => duplicateSelectedNodes(false)}>
                Duplicate without data
              </Button>
            </Dropdown>
            <DropdownItem class="flex items-center justify-between">
              Duplicate selected nodes into another pipeline<ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>
            <Dropdown simple placement="right-start">
              <div class="w-64">
                <SvelteSelect
                  items={pipelines_dropdown}
                  bind:value={selectedPipelineTarget}
                  placeholder="Select a pipeline..."
                  maxItems={5}
                />
              </div>
              <Button
                onclick={() => duplicateSelectedNodesIntoPipeline(true)}
                class="mt-2">Duplicate with data</Button
              >
              <Button
                onclick={() => duplicateSelectedNodesIntoPipeline(false)}
                class="mt-2">Duplicate without data</Button
              >
            </Dropdown>
            <DropdownItem class="flex items-center justify-between">
              Reference selected nodes into another pipeline<ChevronRightOutline
                class="text-primary-700 ms-2 h-6 w-6 dark:text-white"
              />
            </DropdownItem>
            <Dropdown simple placement="right-start">
              <div class="w-64">
                <SvelteSelect
                  items={pipelines_dropdown}
                  bind:value={selectedPipelineTarget}
                  placeholder="Select a pipeline..."
                  maxItems={5}
                />
              </div>
              <Button onclick={referenceSelectedNodesIntoPipeline} class="mt-2"
                >Reference nodes</Button
              >
            </Dropdown>
            <DropdownItem onclick={blockSelectedNodes}  class="text-yellow-600"
              >Block selected nodes</DropdownItem
            >
            <DropdownItem onclick={unblockSelectedNodes}  class="text-yellow-600"
              >Unblock selected nodes</DropdownItem
            >
            <DropdownItem class="text-yellow-600" onclick={blockAllNodesInPipeline}
              >Block all node in current Pipeline</DropdownItem
            >
            <DropdownItem class="text-yellow-600" onclick={unblockAllNodesInPipeline}
              >Unblock block all node in current Pipeline</DropdownItem
            >              
            <DropdownItem class="text-yellow-600" onclick={setNodeCompleted}
              >Manual set node "completed"</DropdownItem
            >
            <DropdownItem class="text-yellow-600" onclick={setNodeStaleData}
              >Manual set node "stale-data"</DropdownItem
            >
            <DropdownItem class="text-red-600" onclick={deleteNodeOutputs}
              >Delete output selected nodes</DropdownItem
            >
            <DropdownItem class="text-red-600" onclick={deleteEdge}
              >Delete selected edge</DropdownItem
            >
            <DropdownItem class="text-red-600" onclick={deleteNode}
              >Delete selected nodes</DropdownItem
            >
          </Dropdown>
          <NavLi class="cursor-pointer">
            Actions<ChevronDownOutline
              class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
            />
          </NavLi>
          <Dropdown simple>
            <DropdownItem onclick={runSelectedNode}
              >Run selected node</DropdownItem
            >
            <DropdownItem onclick={runCurrentPipeline}
              >Run full pipeline</DropdownItem
            >
            <DropdownItem onclick={runPipelineUpToNode}
              >Run pipeline up to selected node</DropdownItem
            >
            <DropdownItem class="text-gray-400 cursor-not-allowed"
              >Open run panel</DropdownItem
            >
            <DropdownItem onclick={killSelectedNode} class="text-red-600">
              Kill run selected node
            </DropdownItem>
          </Dropdown>
          <NavLi class="cursor-pointer">
            Layout<ChevronDownOutline
              class="text-primary-800 ms-2 inline h-6 w-6 dark:text-white"
            />
          </NavLi>
          <Dropdown simple>
            <DropdownItem onclick={() => loadPipeline(currentPipelineId)}>
              Refresh pipeline
            </DropdownItem>
            <DropdownItem onclick={refreshLayout}>Auto reshape</DropdownItem>
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
          <Textarea
            id="node_notes"
            name="node_notes"
            bind:value={nodeDrawereForm.notes}
            rows={4}
            placeholder="Enter notes here..."
          />

          <Label for="node_parameters_yaml" class="mb-2 block"
            >Node Parameters (YAML):</Label
          >
          <Textarea
            id="node_parameters_yaml"
            name="node_parameters_yaml"
            bind:value={nodeDrawereForm.node_parameters_yaml}
            rows={10}
            placeholder="Enter YAML parameters here..."
          />

          <Label for="node_tag" class="mb-2 block">Node Folder path:</Label>
          <div class="mt-2 text-sm text-gray-500">
            {nodeDrawereForm.folder_path || "No folder path"}
          </div>
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
          <Label class="mb-2 block">Project ids:</Label>
          <div class="mt-2 text-sm text-gray-500">
            {pipelineDrawerForm.project_id}
          </div>
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
          <Button onclick={updatePipelineInfo} class="mt-4">Save Changes</Button
          >
        {/if}
      </Drawer>

      <Drawer
        bind:hidden={isHiddenProjectPanel}
        id="projectsidebar"
        aria-controls="projectsidebar"
        aria-labelledby="projectsidebar"
      >
        <div class="flex items-center justify-between">
          <CloseButton
            onclick={() => (isHiddenProjectPanel = false)}
            class="mb-4 dark:text-white"
          />
        </div>
        {#if projectDrawerForm}
          <Label class="mb-2 block">Project id:</Label>
          <div class="mt-2 text-sm text-gray-500">
            {projectDrawerForm.id}
          </div>
          <Label for="project_tag" class="mb-2 block">Project tag:</Label>
          <Input
            id="project_tag"
            name="project_tag"
            required
            bind:value={projectDrawerForm.tag}
          />
          <Label for="project_notes" class="mb-2 block">Notes:</Label>
          <Input
            id="project_notes"
            name="project_notes"
            required
            bind:value={projectDrawerForm.notes}
          />
          <Button class="mt-4" onclick={updateProjectInfo}>Save Changes</Button>
        {/if}
      </Drawer>

      <div class="main-content" bind:clientWidth bind:clientHeight>
        <SvelteFlow
          bind:nodes
          bind:edges
          fitView
          onconnect={handleConnect}
          onnodecontextmenu={handleContextMenu}
          onpaneclick={handlePaneClick}
          onnodedragstop={saveNodePositions}
          {nodeTypes}
          style="height: 100%;"
          disableKeyboardA11y={true}
        >
          <Panel position="top-left">
            Pipeline id: {currentPipelineId || "None"}<br />
            Pipeline tag: {ids_tags_dict_pipelines[currentPipelineId] || "None"}
          </Panel>
          <Controls />
          <Background />
          {#if menu}
            <ContextMenu
              onclick={handlePaneClick}
              id={menu.id}
              top={menu.top}
              left={menu.left}
              {copySelectedNodeFolderPathToClipboard}
            />
          {/if}
          <MiniMap />
        </SvelteFlow>
      </div>
    </div>
  </div>
</div>
