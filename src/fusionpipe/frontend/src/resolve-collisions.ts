import type { Node } from "@xyflow/svelte";

export interface CollisionOptions {
  /** Maximum number of separation passes (default: 20) */
  maxIterations?: number;
  /**
   * Minimum pixel overlap before two nodes are considered colliding (default: 0).
   * Increase to give more breathing room.
   */
  overlapThreshold?: number;
  /** Extra gap (px) added when resolving an overlap (default: 20) */
  margin?: number;
  /** Fallback node width when node.width is not set (default: 172) */
  defaultWidth?: number;
  /** Fallback node height when node.height is not set (default: 36) */
  defaultHeight?: number;
}

interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

function getRect(n: Node, defaultW: number, defaultH: number): Rect {
  return {
    x: n.position.x,
    y: n.position.y,
    w: (n.width as number | undefined) ?? defaultW,
    h: (n.height as number | undefined) ?? defaultH,
  };
}

/**
 * Iteratively push top-level (non-child, non-hidden) nodes apart until there
 * are no more collisions or `maxIterations` is reached.
 *
 * Child nodes (parentId set) and hidden nodes are excluded from movement so
 * that subtree members stay relative to their container.
 *
 * The collision check is purely positional — SvelteFlow's measured dimensions
 * are used when available (`node.width` / `node.height`); otherwise the
 * supplied defaults are used.
 *
 * Usage (e.g. on collapse/expand or drag-stop):
 * ```ts
 * nodes = resolveCollisions(nodes, { margin: 20 });
 * ```
 */
export function resolveCollisions(
  nodes: Node[],
  options: CollisionOptions = {},
): Node[] {
  const {
    maxIterations = 20,
    overlapThreshold = 0,
    margin = 20,
    defaultWidth = 172,
    defaultHeight = 36,
  } = options;

  // Work only with top-level, visible nodes
  const movableIds = new Set(
    nodes
      .filter((n) => !n.parentId && !n.hidden)
      .map((n) => n.id),
  );

  if (movableIds.size < 2) return nodes;

  // Clone positions so we can mutate freely without affecting the original array
  const pos: Map<string, { x: number; y: number }> = new Map(
    nodes.map((n) => [n.id, { x: n.position.x, y: n.position.y }]),
  );

  // Dims are read-only during the loop
  const dim: Map<string, { w: number; h: number }> = new Map(
    nodes.map((n) => [
      n.id,
      {
        w: (n.width as number | undefined) ?? defaultWidth,
        h: (n.height as number | undefined) ?? defaultHeight,
      },
    ]),
  );

  const movable = nodes.filter((n) => movableIds.has(n.id));

  let anyMoved = true;
  let iter = 0;

  while (anyMoved && iter < maxIterations) {
    anyMoved = false;
    iter++;

    for (let i = 0; i < movable.length; i++) {
      for (let j = i + 1; j < movable.length; j++) {
        const idA = movable[i].id;
        const idB = movable[j].id;

        const pA = pos.get(idA)!;
        const pB = pos.get(idB)!;
        const dA = dim.get(idA)!;
        const dB = dim.get(idB)!;

        const overlapX = Math.min(pA.x + dA.w, pB.x + dB.w) - Math.max(pA.x, pB.x);
        const overlapY = Math.min(pA.y + dA.h, pB.y + dB.h) - Math.max(pA.y, pB.y);

        if (overlapX > overlapThreshold && overlapY > overlapThreshold) {
          // Resolve along the axis with the smaller overlap (minimum displacement)
          if (overlapX <= overlapY) {
            const shift = (overlapX + margin) / 2;
            const dir = pA.x <= pB.x ? 1 : -1;
            pA.x -= dir * shift;
            pB.x += dir * shift;
          } else {
            const shift = (overlapY + margin) / 2;
            const dir = pA.y <= pB.y ? 1 : -1;
            pA.y -= dir * shift;
            pB.y += dir * shift;
          }
          anyMoved = true;
        }
      }
    }
  }

  // Produce an updated nodes array with the new positions
  return nodes.map((n) => {
    if (!movableIds.has(n.id)) return n;
    const p = pos.get(n.id)!;
    if (p.x === n.position.x && p.y === n.position.y) return n;
    return { ...n, position: { x: p.x, y: p.y } };
  });
}
