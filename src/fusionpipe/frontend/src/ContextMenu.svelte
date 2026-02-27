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
        // Subtree actions
        createSubtree,
        toggleSubtreeCollapse,
        deleteSubtree,
        // Context about the right-clicked node relative to subtrees
        nodeSubtreeId,   // string | null — subtree the node belongs to (or the node IS a container)
        subtreeCollapsed, // boolean
        canCreateSubtree, // boolean — false if a selected node is already in a subtree
    }: {
        id: string;
        top: number | undefined;
        left: number | undefined;
        right: number | undefined;
        bottom: number | undefined;
        onclick: () => void;
        copySelectedNodeFolderPathToClipboard: () => void;
        createSubtree: () => void;
        toggleSubtreeCollapse: (subtreeId: string) => void;
        deleteSubtree: (subtreeId: string) => void;
        nodeSubtreeId: string | null;
        subtreeCollapsed: boolean;
        canCreateSubtree: boolean;
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
    class:disabled={!canCreateSubtree}
    title={canCreateSubtree ? "Group selected nodes into a visual subtree" : "A selected node is already in a subtree"}
    onclick={(e) => { e.stopPropagation(); if (canCreateSubtree) createSubtree(); }}
  >
    Create Subtree from Selected
  </button>
  {#if nodeSubtreeId}
    <button onclick={(e) => { e.stopPropagation(); toggleSubtreeCollapse(nodeSubtreeId!); }}>
      {subtreeCollapsed ? "Expand Subtree" : "Collapse Subtree"}
    </button>
    <button class="danger" onclick={(e) => { e.stopPropagation(); deleteSubtree(nodeSubtreeId!); }}>
      Remove Subtree
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

