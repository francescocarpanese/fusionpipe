<script lang="ts">
  import {
    type EdgeProps,
    getBezierPath,
    BaseEdge,
    EdgeLabel,
  } from '@xyflow/svelte';
 
  let {
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    data,
  }: EdgeProps = $props();
 
  let [edgePath, labelX, labelY] = $derived(
    getBezierPath({
      sourceX,
      sourceY,
      sourcePosition,
      targetX,
      targetY,
      targetPosition,
    }),
  );
</script>
 
<BaseEdge path={edgePath} />
<EdgeLabel x={labelX} y={labelY}>
  <div class="edge-label">
    {data.label}
  </div>
</EdgeLabel>
 
<style>
  .edge-label {
    background-color: #2a6042bf;
    color: #000;
    padding: 2px;
    border-radius: 5px;
    font-size: 8px;
    font-weight: 500;
  }
</style>