<script lang="ts">
    import { Handle, Position, type NodeProps } from '@xyflow/svelte';
    let { data }: NodeProps = $props();
    let selected
</script>


<div class="custom-node">
    {#if !data.editable}
        <img src="/Lock.jpg" alt="Locked" width="24" height="24" />
    {/if}
    <span>{data.label}</span>
    <Handle type="target" position={Position.Top} />
    <Handle type="source" position={Position.Bottom} />
</div>

<!-- <style>
    .custom-node {
        display: flex;
        align-items: center;
        gap: 8px;
    }
</style> -->
