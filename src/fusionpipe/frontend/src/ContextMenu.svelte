<script lang="ts">
    import { useEdges, useNodes, useSvelteFlow } from "@xyflow/svelte";

    let {
        id,
        top,
        left,
        right,
        bottom,
        onclick,
        copySelectedNodeFolderPathToClipboard,
        // Node group actions
        createNodeGroup,
        toggleGroupCollapse,
        deleteNodeGroup,
        // Context about the right-clicked node relative to node groups
        nodeGroupId,      // string | null — group the node belongs to (or the node IS a container)
        groupCollapsed,   // boolean
        canCreateGroup,   // boolean — false if a selected node is already in a group
    }: {
        id: string;
        top: number | undefined;
        left: number | undefined;
        right: number | undefined;
        bottom: number | undefined;
        onclick: () => void;
        copySelectedNodeFolderPathToClipboard: () => void;
        createNodeGroup: () => void;
        toggleGroupCollapse: (groupId: string) => void;
        deleteNodeGroup: (groupId: string) => void;
        nodeGroupId: string | null;
        groupCollapsed: boolean;
        canCreateGroup: boolean;
    } = $props();
</script>

<div
  style="top: {top}px; left: {left}px; position: fixed;"
  class="context-menu"
  onclick={onclick}
>
  <button onclick={(e) => { e.stopPropagation(); copySelectedNodeFolderPathToClipboard(); }}>Copy Folder Path</button>
  <hr class="divider" />
  <button
    class:disabled={!canCreateGroup}
    title={canCreateGroup ? "Group selected nodes into a visual node group" : "A selected node is already in a group"}
    onclick={(e) => { e.stopPropagation(); if (canCreateGroup) createNodeGroup(); }}
  >
    Create Node Group from Selected
  </button>
  {#if nodeGroupId}
    <button onclick={(e) => { e.stopPropagation(); toggleGroupCollapse(nodeGroupId!); }}>
      {groupCollapsed ? "Expand Group" : "Collapse Group"}
    </button>
    <button class="danger" onclick={(e) => { e.stopPropagation(); deleteNodeGroup(nodeGroupId!); }}>
      Remove Node Group
    </button>
  {/if}
</div>

<style>
    .context-menu {
        background: white;
        border-style: solid;
        box-shadow: 10px 19px 20px rgba(0, 0, 0, 10%);
        position: absolute;
        z-index: 10;
        min-width: 200px;
    }

    .context-menu button {
        border: none;
        display: block;
        padding: 0.5em 0.75em;
        text-align: left;
        width: 100%;
        cursor: pointer;
        background: white;
        font-size: 13px;
    }

    .context-menu button:hover {
        background: #f3f4f6;
    }

    .context-menu button.disabled {
        color: #9ca3af;
        cursor: not-allowed;
    }

    .context-menu button.disabled:hover {
        background: white;
    }

    .context-menu button.danger {
        color: #b91c1c;
    }

    .context-menu button.danger:hover {
        background: #fee2e2;
    }

    .divider {
        border: none;
        border-top: 1px solid #e5e7eb;
        margin: 2px 0;
    }
</style>

