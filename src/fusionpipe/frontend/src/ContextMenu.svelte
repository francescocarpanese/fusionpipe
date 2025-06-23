<script lang="ts">
    import { useEdges, useNodes, useSvelteFlow } from "@xyflow/svelte";

    let {
        id,
        top,
        left,
        right,
        bottom,
        onclick,
        copySelectedNodeFolderPathToClipboard
    }: {
        id: string;
        top: number | undefined;
        left: number | undefined;
        right: number | undefined;
        bottom: number | undefined;
        onclick: () => void;
        copySelectedNodeFolderPathToClipboard: () => void;
    } = $props();
</script>

<div
  style="top: {top}px; left: {left}px; position: fixed;"
  class="context-menu"
  on:click={onclick}
>
  <button on:click|stopPropagation={copySelectedNodeFolderPathToClipboard}>Copy Folder Path</button>
</div>

<style>
    .context-menu {
        background: white;
        border-style: solid;
        box-shadow: 10px 19px 20px rgba(0, 0, 0, 10%);
        position: absolute;
        z-index: 10;
    }

    .context-menu button {
        border: none;
        display: block;
        padding: 0.5em;
        text-align: left;
        width: 100%;
    }

    .context-menu button:hover {
        background: white;
    }
</style>
