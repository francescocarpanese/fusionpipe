<script lang="ts">
    import { Handle, Position, type NodeProps } from "@xyflow/svelte";
    let { data }: NodeProps = $props();
</script>

<div class="custom-node">
    <div class="image-row">
        {#if data.blocked}
            <img src="/Lock.jpg" alt="Blocked" width="14" height="14" />
        {/if}
    </div> 
    <span class="line">{data.line1}</span>
    <span class="line">{data.line2}</span>
    <Handle type="target" position={Position.Top} />
    <Handle type="source" position={Position.Bottom} />
</div>
