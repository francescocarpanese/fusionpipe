<script lang="ts">
  import { SvelteFlow, Controls, Panel, type Node, type Edge } from "@xyflow/svelte";
  export let nodes: Node[] = [];
  export let edges: Edge[] = [];
  export let currentProjectId;
  export let ids_tags_dict_projects;
  export let nodeTypes;
  export let onUpdate: ((event: { detail: { nodes: Node[]; edges: Edge[] } }) => void) | undefined;

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
    // Mark the clicked node as selected, others as not
    nodes = nodes.map(node =>
      node.id === e.node.id
        ? { ...node, selected: true }
        : { ...node, selected: false }
    );
    emitUpdate();
  }}
>
  <Panel position="top-left">
    Project id: {currentProjectId || "None"}<br />
    Project tag: {ids_tags_dict_projects[currentProjectId] || "None"}<br />
  </Panel>
  <Controls />
</SvelteFlow>