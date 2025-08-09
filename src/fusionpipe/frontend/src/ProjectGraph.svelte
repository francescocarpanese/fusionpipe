<script lang="ts">
  import { SvelteFlow, Controls, Panel, type Node, type Edge } from "@xyflow/svelte";
  export let nodes: Node[] = [];
  export let edges: Edge[] = [];
  export let currentProjectId;
  export let ids_tags_dict_projects;
  export let nodeTypes;
  export let onUpdate: ((event: { detail: { nodes: Node[]; edges: Edge[] } }) => void) | undefined;
  export let onLoadPipeline: (pipelineId: string) => void = () => {};
  export let onOpenPipelinePanel: (pipelineId: string) => void = () => {};
  export let onBlockPipeline: (pipelineId: string) => void = () => {};
  export let onUnblockPipeline: (pipelineId: string) => void = () => {};

  let menu: { id: string; top: number; left: number } | null = null;

  // Handle right-click on node
  function handleNodeContextMenu(e) {
    e.event.preventDefault();
    menu = {
      id: e.node.id,
      top: e.event.clientY,
      left: e.event.clientX,
    };
    // Mark the node as selected
    nodes = nodes.map(node =>
      node.id === e.node.id
        ? { ...node, selected: true }
        : { ...node, selected: false }
    );
    emitUpdate();
  }

  function handlePaneClick() {
    menu = null;
  }

  // Example: handle menu option
  function handleMenuOption(option) {
    if (menu && menu.id) {
      if (option === 'loadPipeline') {
        onLoadPipeline(menu.id);
      } else if (option === 'openPipelinePanel') {
        onOpenPipelinePanel(menu.id);
      } else if (option === 'blockPipeline') {
        onBlockPipeline(menu.id);
      } else if (option === 'unblockPipeline') {
        onUnblockPipeline(menu.id);
      }
    }
    menu = null;
  }

  // Emit changes to parent
  function emitUpdate() {
    onUpdate?.({ detail: { nodes, edges } });
  }
</script>

<SvelteFlow
  bind:nodes
  bind:edges
  fitView
  {nodeTypes}
  style="height: 100%;"
  onnodeclick={(e) => {
    nodes = nodes.map(node =>
      node.id === e.node.id
        ? { ...node, selected: true }
        : { ...node, selected: false }
    );
    emitUpdate();
  }}
  onnodecontextmenu={handleNodeContextMenu}
  onpaneclick={handlePaneClick}
>
  <Panel position="top-left">
    Project ID: {currentProjectId || "None"}<br />
    Project TAG: {ids_tags_dict_projects[currentProjectId] || "None"}<br />
  </Panel>
  <Controls />
  {#if menu}
    <div
      class="absolute bg-white border rounded shadow z-50"
      style="top: {menu.top}px; left: {menu.left}px; min-width: 180px;"
    >
      <button
        class="block px-4 py-2 hover:bg-gray-100 w-full text-left"
        on:click={() => handleMenuOption('loadPipeline')}
      >
        Activate Selected Pipeline
      </button>
      <button
        class="block px-4 py-2 hover:bg-gray-100 w-full text-left"
        on:click={() => handleMenuOption('openPipelinePanel')}
      >
        Open Active Pipeline Panel
      </button>
    </div>
  {/if}
</SvelteFlow>