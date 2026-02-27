<script lang="ts">
  import { Handle, Position, NodeResizer } from "@xyflow/svelte";

  let {
    id,
    selected,
    data,
  }: {
    id: string;
    selected: boolean;
    data: {
      tag: string;
      collapsed: boolean;
      subtreeId: string;
      onToggleCollapse: (subtreeId: string) => void;
      onDeleteSubtree: (subtreeId: string) => void;
    };
  } = $props();

  function handleToggle(e: MouseEvent) {
    e.stopPropagation();
    data.onToggleCollapse(data.subtreeId);
  }

  function handleDelete(e: MouseEvent) {
    e.stopPropagation();
    if (confirm(`Remove subtree "${data.tag || data.subtreeId}"?\nMember nodes will remain in the pipeline.`)) {
      data.onDeleteSubtree(data.subtreeId);
    }
  }
</script>

<!-- Resizer is only shown when expanded and selected -->
<NodeResizer
  minWidth={120}
  minHeight={60}
  isVisible={selected && !data.collapsed}
/>

<!-- Target handle (visible only when collapsed) -->
<Handle
  type="target"
  position={Position.Top}
  style={data.collapsed ? "" : "opacity: 0; pointer-events: none;"}
/>

<div class="subtree-wrapper" class:collapsed={data.collapsed}>
  <div class="subtree-header" class:header-only={data.collapsed}>
    <span class="subtree-label" title={data.tag || "Subtree"}>
      {data.tag || "Subtree"}
    </span>
    <div class="subtree-actions">
      <button class="subtree-btn" onclick={handleToggle} title={data.collapsed ? "Expand subtree" : "Collapse subtree"}>
        {data.collapsed ? "▶" : "▼"}
      </button>
      <button class="subtree-btn delete-btn" onclick={handleDelete} title="Remove subtree grouping">
        ✕
      </button>
    </div>
  </div>
  {#if !data.collapsed}
    <div class="subtree-body"></div>
  {/if}
</div>

<!-- Source handle (visible only when collapsed) -->
<Handle
  type="source"
  position={Position.Bottom}
  style={data.collapsed ? "" : "opacity: 0; pointer-events: none;"}
/>

<style>
  .subtree-wrapper {
    width: 100%;
    height: 100%;
    border: 2px dashed #6366f1;
    border-radius: 8px;
    background: rgba(99, 102, 241, 0.04);
    display: flex;
    flex-direction: column;
    box-sizing: border-box;
    overflow: hidden;
  }

  .subtree-wrapper.collapsed {
    background: rgba(99, 102, 241, 0.14);
    border-style: solid;
    border-radius: 20px;
  }

  .subtree-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 8px;
    background: rgba(99, 102, 241, 0.18);
    border-radius: 6px 6px 0 0;
    min-height: 28px;
    flex-shrink: 0;
    gap: 6px;
  }

  .subtree-header.header-only {
    border-radius: 18px;
    min-height: 32px;
  }

  .subtree-label {
    font-size: 11px;
    font-weight: 600;
    color: #3730a3;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex: 1;
    min-width: 0;
  }

  .subtree-actions {
    display: flex;
    gap: 2px;
    flex-shrink: 0;
  }

  .subtree-btn {
    border: none;
    background: transparent;
    cursor: pointer;
    padding: 1px 5px;
    border-radius: 4px;
    font-size: 10px;
    color: #3730a3;
    line-height: 1.4;
  }

  .subtree-btn:hover {
    background: rgba(99, 102, 241, 0.3);
  }

  .delete-btn {
    color: #b91c1c;
  }

  .delete-btn:hover {
    background: rgba(185, 28, 28, 0.1);
  }

  .subtree-body {
    flex: 1;
  }
</style>
