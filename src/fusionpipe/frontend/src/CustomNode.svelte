<script lang="ts">
    import { Handle, Position, type NodeProps } from "@xyflow/svelte";
    let { data }: NodeProps = $props();
</script>

<div class="custom-node">
    {#if data.referenced}
        <img src="/Lock.jpg" alt="Locked" width="12" height="12" />
    {/if}
    <span class="line">{data.line1}</span>
    <span class="line">{data.line2}</span>
    <Handle type="target" position={Position.Top} />
    <Handle type="source" position={Position.Bottom} />
</div>
