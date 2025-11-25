import {
  ConvexError,
  convexToJson,
  type Value as ConvexValue,
  jsonToConvex,
  v,
  type GenericId,
} from "convex/values";
import {
  type DatabaseReader,
  type DatabaseWriter,
  internalMutation,
  query,
} from "./_generated/server.js";
import type { Doc, Id } from "./_generated/dataModel.js";
import { compareValues } from "./compare.js";
import {
  type Item,
  itemValidator,
} from "./schema.js";
import { internal } from "./_generated/api.js";

const BTREE_DEBUG = false;
export const DEFAULT_MAX_NODE_SIZE = 16;

export type Key = ConvexValue;
// Value is always a Convex ID.
export type Value = GenericId<string>;
export type Namespace = ConvexValue | undefined;

export function p(v: ConvexValue): string {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function log(s: string) {
  if (BTREE_DEBUG) {
    console.log(s);
  }
}

/**
 * Inserts a key-value pair into the B-tree.
 * If the tree doesn't exist, it creates it.
 * If the insertion causes the root to split, it creates a new root.
 */
export async function insertHandler(
  ctx: { db: DatabaseWriter },
  args: { key: Key; value: Value; namespace?: Namespace },
) {
  const tree = await getOrCreateTree(
    ctx.db,
    args.namespace,
    DEFAULT_MAX_NODE_SIZE,
  );
  // Attempt to insert into the root node.
  // If the root is full, it might return a 'PushUp' object containing the median item
  // and the two split nodes.
  const pushUp = await insertIntoNode(ctx, args.namespace, tree.root, {
    k: args.key,
    v: args.value,
  });

  // If the root split, create a new root node.
  if (pushUp) {
    const newRoot = await ctx.db.insert("btreeNode", {
      items: [pushUp.item],
      subtrees: [pushUp.leftSubtree, pushUp.rightSubtree],
    });
    await ctx.db.patch(tree._id, {
      root: newRoot,
    });
  }
}

/**
 * Deletes a key from the B-tree.
 * If the deletion causes the root to become empty (but with one child),
 * it collapses the root to reduce tree height.
 */
export async function deleteHandler(
  ctx: { db: DatabaseWriter },
  args: { key: Key; namespace?: Namespace },
) {
  const tree = await getOrCreateTree(
    ctx.db,
    args.namespace,
    DEFAULT_MAX_NODE_SIZE,
  );

  // Perform the deletion starting from the root.
  await deleteFromNode(ctx, args.namespace, tree.root, args.key);

  // Check if the root can be collapsed.
  const root = (await ctx.db.get(tree.root))!;
  if (root.items.length === 0 && root.subtrees.length === 1) {
    log(
      `collapsing root ${root._id} because its only child is ${root.subtrees[0]}`,
    );
    // Make the only child the new root.
    await ctx.db.patch(tree._id, {
      root: root.subtrees[0],
    });
    // Delete the old empty root.
    await ctx.db.delete(root._id);
  }
}

export const validate = query({
  args: { namespace: v.optional(v.any()) },
  handler: validateTree,
});

/**
 * Validates the structural integrity of the B-tree.
 * Checks for ordering, size constraints, and height balance.
 */
export async function validateTree(
  ctx: { db: DatabaseReader },
  args: { namespace?: Namespace },
) {
  const tree = await getTree(ctx.db, args.namespace);
  if (!tree) {
    return;
  }
  await validateNode(ctx, args.namespace, tree.root, 0);
}

type ValidationResult = {
  min?: Key;
  max?: Key;
  height: number;
};

async function MAX_NODE_SIZE(
  ctx: { db: DatabaseReader },
  namespace: Namespace,
) {
  const tree = await mustGetTree(ctx.db, namespace);
  return tree.maxNodeSize;
}

async function MIN_NODE_SIZE(
  ctx: { db: DatabaseReader },
  namespace: Namespace,
) {
  const max = await MAX_NODE_SIZE(ctx, namespace);
  if (max % 2 !== 0 || max < 4) {
    throw new Error("MAX_NODE_SIZE must be even and at least 4");
  }
  return max / 2;
}

/**
 * Recursively validates a single node and its subtrees.
 */
async function validateNode(
  ctx: { db: DatabaseReader },
  namespace: Namespace,
  node: Id<"btreeNode">,
  depth: number,
): Promise<ValidationResult> {
  const n = await ctx.db.get(node);
  if (!n) {
    throw new ConvexError(`missing node ${node}`);
  }
  // Check max node size invariant.
  if (n.items.length > (await MAX_NODE_SIZE(ctx, namespace))) {
    throw new ConvexError(`node ${node} exceeds max size`);
  }
  // Check min node size invariant (except for root).
  if (depth > 0 && n.items.length < (await MIN_NODE_SIZE(ctx, namespace))) {
    throw new ConvexError(`non-root node ${node} has less than min-size`);
  }
  // Check that number of keys + 1 == number of subtrees (if not a leaf).
  if (n.subtrees.length > 0 && n.items.length + 1 !== n.subtrees.length) {
    throw new ConvexError(`node ${node} keys do not match subtrees`);
  }
  if (n.subtrees.length > 0 && n.items.length === 0) {
    throw new ConvexError(`node ${node} one subtree but no keys`);
  }
  // Keys must be sorted.
  for (let i = 1; i < n.items.length; i++) {
    if (compareKeys(n.items[i - 1].k, n.items[i].k) !== -1) {
      throw new ConvexError(`node ${node} keys not in order`);
    }
  }
  // Recursively validate subtrees.
  const validatedSubtrees = await Promise.all(
    n.subtrees.map((subtree) =>
      validateNode(ctx, namespace, subtree, depth + 1),
    ),
  );
  for (let i = 0; i < n.subtrees.length; i++) {
    // Each subtree's min is greater than the key at the prior index
    if (
      i > 0 &&
      compareKeys(validatedSubtrees[i].min!, n.items[i - 1].k) !== 1
    ) {
      throw new ConvexError(`subtree ${i} min is too small for node ${node}`);
    }
    // Each subtree's max is less than the key at the same index
    if (
      i < n.items.length &&
      compareKeys(validatedSubtrees[i].max!, n.items[i].k) !== -1
    ) {
      throw new ConvexError(`subtree ${i} max is too large for node ${node}`);
    }
  }
  // All subtrees have the same height.
  const heights = validatedSubtrees.map((s) => s.height);
  for (let i = 1; i < heights.length; i++) {
    if (heights[i] !== heights[0]) {
      throw new ConvexError(`subtree ${i} has different height from others`);
    }
  }

  // Determine min and max keys for this subtree.
  const max =
    validatedSubtrees.length > 0
      ? validatedSubtrees[validatedSubtrees.length - 1].max
      : n.items[n.items.length - 1]?.k;
  const min =
    validatedSubtrees.length > 0 ? validatedSubtrees[0].min : n.items[0]?.k;
  const height = validatedSubtrees.length > 0 ? 1 + heights[0] : 0;
  return { min, max, height };
}

type WithinBounds =
  | {
    type: "item";
    item: Item;
  }
  | {
    type: "subtree";
    subtree: Id<"btreeNode">;
  };

/**
 * Finds items and subtrees within a node that fall within the given key range [k1, k2].
 * Used for range queries and pagination.
 */
async function filterBetween(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  k1?: Key,
  k2?: Key,
): Promise<WithinBounds[]> {
  const n = (await db.get(node))!;
  const included: (WithinBounds | Promise<WithinBounds[]>)[] = [];

  // Helper to recursively include a subtree if it overlaps with the range.
  function includeSubtree(i: number, unboundedRight: boolean) {
    const unboundedLeft = k1 === undefined || included.length > 0;
    if (unboundedLeft && unboundedRight) {
      // If fully within range, include the whole subtree.
      included.push({ type: "subtree", subtree: n.subtrees[i] });
    } else {
      // Otherwise, recurse to find partial matches.
      included.push(
        filterBetween(
          db,
          n.subtrees[i],
          unboundedLeft ? undefined : k1,
          unboundedRight ? undefined : k2,
        ),
      );
    }
  }
  let done = false;
  for (let i = 0; i < n.items.length; i++) {
    const k1IsLeft = k1 === undefined || compareKeys(k1, n.items[i].k) === -1;
    const k2IsRight = k2 === undefined || compareKeys(k2, n.items[i].k) === 1;

    // Check the subtree to the left of the current item.
    if (k1IsLeft && n.subtrees.length > 0) {
      includeSubtree(i, k2IsRight);
    }
    if (!k2IsRight) {
      // We've reached the right bound, so we're done.
      done = true;
      break;
    }
    // If the item is within range, include it.
    if (k1IsLeft) {
      included.push({ type: "item", item: n.items[i] as Item });
    }
  }
  // Check the rightmost subtree if we haven't finished.
  if (!done && n.subtrees.length > 0) {
    includeSubtree(n.subtrees.length - 1, k2 === undefined);
  }
  return (await Promise.all(included)).flat(1);
}

export async function getHandler(
  ctx: { db: DatabaseReader },
  args: { key: Key; namespace?: Namespace },
) {
  const tree = (await getTree(ctx.db, args.namespace))!;
  return await getInNode(ctx.db, tree.root, args.key);
}

export const get = query({
  args: { key: v.any(), namespace: v.optional(v.any()) },
  returns: v.union(v.null(), itemValidator),
  handler: getHandler,
});

/**
 * Recursively searches for a key in a node and its subtrees.
 */
async function getInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  key: Key,
): Promise<Item | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(key, n.items[i].k);
    if (compare === -1) {
      // if key < n.items[i].k, recurse to the left of index i
      break;
    }
    if (compare === 0) {
      // Found the key!
      return n.items[i] as Item;
    }
  }
  // If not found in items, check subtrees.
  if (n.subtrees.length === 0) {
    // Leaf node and key not found.
    return null;
  }
  return await getInNode(db, n.subtrees[i], key);
}

/**
 * Recursively deletes a key from a node.
 * Handles rebalancing (rotation and merging) if a subtree becomes deficient.
 */
async function deleteFromNode(
  ctx: { db: DatabaseWriter },
  namespace: Namespace,
  node: Id<"btreeNode">,
  key: Key,
): Promise<Item | null> {
  let n = (await ctx.db.get(node))!;
  let foundItem: null | Item = null;
  let i = 0;

  // Find the key or the subtree it might be in.
  for (; i < n.items.length; i++) {
    const compare = compareKeys(key, n.items[i].k);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      log(`found key ${p(key)} in node ${n._id}`);
      // we've found the key. delete it.

      // CASE 1: Leaf Node
      if (n.subtrees.length === 0) {
        // if this is a leaf node, just delete the key
        await ctx.db.patch(node, {
          items: [...n.items.slice(0, i), ...n.items.slice(i + 1)],
        });
        return n.items[i] as Item;
      }

      // CASE 2: Internal Node
      // Replace the key with its predecessor (the max key from the left subtree).
      const predecessor = await findMax(ctx.db, n.subtrees[i]);
      log(`replacing ${p(key)} with predecessor ${p(predecessor.k)}`);
      foundItem = n.items[i] as Item;

      // Temporarily put the predecessor in the current slot.
      await ctx.db.patch(node, {
        items: [...n.items.slice(0, i), predecessor, ...n.items.slice(i + 1)],
      });
      n = (await ctx.db.get(node))!;

      // Now recursively delete the original predecessor from the left subtree.
      // We change 'key' to 'predecessor.k' so the recursive call finds it.
      key = predecessor.k;
      break;
    }
  }

  // If key not found in this node, it must be in subtree i.
  if (n.subtrees.length === 0) {
    throw new ConvexError({
      code: "DELETE_MISSING_KEY",
      message: `key ${p(key)} not found in node ${n._id}`,
    });
  }

  // Recursive delete from the child.
  const deleted = await deleteFromNode(ctx, namespace, n.subtrees[i], key);
  if (!deleted) {
    return null;
  }
  if (!foundItem) {
    foundItem = deleted;
  }

  // Rebalancing: Check if the subtree at index i is too small.
  const deficientSubtree = (await ctx.db.get(n.subtrees[i]))!;
  const minNodeSize = await MIN_NODE_SIZE(ctx, namespace);

  if (deficientSubtree.items.length < minNodeSize) {
    log(`deficient subtree ${deficientSubtree._id}`);

    // Try to fix deficiency by rotating from siblings.

    // Rotation Option 1: Rotate Right (borrow from left sibling)
    if (i > 0) {
      const leftSibling = (await ctx.db.get(n.subtrees[i - 1]))!;
      if (leftSibling.items.length > minNodeSize) {
        log(`rotating right with left sibling ${leftSibling._id}`);

        // Move rightmost child of left sibling to deficient subtree (if internal node)
        const grandchild = leftSibling.subtrees.length
          ? await ctx.db.get(
            leftSibling.subtrees[leftSibling.subtrees.length - 1],
          )
          : null;

        // Move separator from parent to deficient subtree
        // Move rightmost item of left sibling to parent
        await ctx.db.patch(deficientSubtree._id, {
          items: [n.items[i - 1], ...deficientSubtree.items],
          subtrees: grandchild
            ? [grandchild._id, ...deficientSubtree.subtrees]
            : [],
        });
        await ctx.db.patch(leftSibling._id, {
          items: leftSibling.items.slice(0, leftSibling.items.length - 1),
          subtrees: grandchild
            ? leftSibling.subtrees.slice(0, leftSibling.subtrees.length - 1)
            : [],
        });
        await ctx.db.patch(node, {
          items: [
            ...n.items.slice(0, i - 1),
            leftSibling.items[leftSibling.items.length - 1],
            ...n.items.slice(i),
          ],
        });
        return foundItem;
      }
    }

    // Rotation Option 2: Rotate Left (borrow from right sibling)
    if (i < n.subtrees.length - 1) {
      const rightSibling = (await ctx.db.get(n.subtrees[i + 1]))!;
      if (rightSibling.items.length > minNodeSize) {
        log(`rotating left with right sibling ${rightSibling._id}`);

        // Move leftmost child of right sibling to deficient subtree
        const grandchild = rightSibling.subtrees.length
          ? await ctx.db.get(rightSibling.subtrees[0])
          : null;

        // Move separator from parent to deficient subtree
        // Move leftmost item of right sibling to parent
        await ctx.db.patch(deficientSubtree._id, {
          items: [...deficientSubtree.items, n.items[i]],
          subtrees: grandchild
            ? [...deficientSubtree.subtrees, grandchild._id]
            : [],
        });
        await ctx.db.patch(rightSibling._id, {
          items: rightSibling.items.slice(1),
          subtrees: grandchild ? rightSibling.subtrees.slice(1) : [],
        });
        await ctx.db.patch(node, {
          items: [
            ...n.items.slice(0, i),
            rightSibling.items[0],
            ...n.items.slice(i + 1),
          ],
        });
        return foundItem;
      }
    }

    // Merge Option: If can't rotate, merge with a sibling.
    if (i > 0) {
      log(`merging with left sibling`);
      await mergeNodes(ctx.db, n, i - 1);
    } else {
      log(`merging with right sibling`);
      await mergeNodes(ctx.db, n, i);
    }
  }
  return foundItem;
}

/**
 * Merges two sibling nodes and the separator key from the parent into one node.
 * Deletes the right sibling.
 */
async function mergeNodes(
  db: DatabaseWriter,
  parent: Doc<"btreeNode">,
  leftIndex: number,
) {
  const left = (await db.get(parent.subtrees[leftIndex]))!;
  const right = (await db.get(parent.subtrees[leftIndex + 1]))!;
  log(`merging ${right._id} into ${left._id}`);

  // Combine left + parent separator + right
  await db.patch(left._id, {
    items: [...left.items, parent.items[leftIndex], ...right.items],
    subtrees: [...left.subtrees, ...right.subtrees],
  });

  // Remove separator and right sibling pointer from parent
  await db.patch(parent._id, {
    items: [
      ...parent.items.slice(0, leftIndex),
      ...parent.items.slice(leftIndex + 1),
    ],
    subtrees: [
      ...parent.subtrees.slice(0, leftIndex + 1),
      ...parent.subtrees.slice(leftIndex + 2),
    ],
  });
  // Delete the now-empty right sibling
  await db.delete(right._id);
}

/**
 * Finds the maximum item in a subtree (used for finding predecessors).
 */
async function findMax(
  db: DatabaseReader,
  node: Id<"btreeNode">,
): Promise<Item> {
  const n = (await db.get(node))!;
  if (n.subtrees.length > 0) {
    // Recurse to the rightmost child
    return findMax(db, n.subtrees[n.subtrees.length - 1]);
  }
  // Return the rightmost item
  return n.items[n.items.length - 1] as Item;
}

type PushUp = {
  leftSubtree: Id<"btreeNode">;
  rightSubtree: Id<"btreeNode">;
  item: Item;
};

/**
 * Recursively inserts an item into a node.
 * Returns a PushUp object if the node splits.
 */
async function insertIntoNode(
  ctx: { db: DatabaseWriter },
  namespace: Namespace,
  node: Id<"btreeNode">,
  item: Item,
): Promise<PushUp | null> {
  // 1. Load the current node from the database
  const n = (await ctx.db.get(node))!;

  // 2. Find the correct index 'i' where the new item should go
  // We iterate until we find a key in the node that is greater than our item's key.
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(item.k, n.items[i].k);
    if (compare === -1) {
      // item.key < n.items[i].key, so we found the spot (before index i)
      break;
    }
    if (compare === 0) {
      throw new ConvexError(`key ${p(item.k)} already exists in node ${n._id}`);
    }
  }

  // 3. Handle Insertion
  if (n.subtrees.length > 0) {
    // CASE A: Internal Node (has subtrees)
    // We don't insert here directly yet. We recurse into the appropriate child.
    // The child at index 'i' covers the range (-inf, n.items[i]) or (n.items[i-1], n.items[i]).

    // Recursively insert into the child
    const pushUp = await insertIntoNode(ctx, namespace, n.subtrees[i], item);

    // If the child split and pushed an item up...
    if (pushUp) {
      // We insert the pushed-up item into *this* node at index 'i'.
      // We also replace the old child pointer (subtrees[i]) with the two new pointers 
      // (pushUp.leftSubtree and pushUp.rightSubtree).
      await ctx.db.patch(node, {
        items: [...n.items.slice(0, i), pushUp.item, ...n.items.slice(i)],
        subtrees: [
          ...n.subtrees.slice(0, i),
          pushUp.leftSubtree,
          pushUp.rightSubtree,
          ...n.subtrees.slice(i + 1),
        ],
      });
    }
    // If pushUp is null, the child absorbed the item without splitting, so we do nothing.
  } else {
    // CASE B: Leaf Node
    // Just insert the item directly into the items array at index 'i'.
    await ctx.db.patch(node, {
      items: [...n.items.slice(0, i), item, ...n.items.slice(i)],
    });
  }

  // 4. Check for Overflow (The "Push Up" Mechanism)
  // We reload the node to get the latest state (with the new item inserted).
  const newN = (await ctx.db.get(node))!;
  const maxNodeSize = await MAX_NODE_SIZE(ctx, namespace);
  const minNodeSize = await MIN_NODE_SIZE(ctx, namespace);

  // If the node is now too big...
  if (newN.items.length > maxNodeSize) {
    // Sanity checks for B-tree invariants
    if (
      newN.items.length !== maxNodeSize + 1 ||
      newN.items.length !== 2 * minNodeSize + 1
    ) {
      throw new Error(`bad ${newN.items.length}`);
    }
    log(`splitting node ${newN._id} at ${newN.items[minNodeSize].k}`);

    // SPLIT OPERATION:
    // We keep the left half in the current node.
    // We move the right half to a new sibling node.
    // The middle item (at minNodeSize) is "pushed up".

    // 4a. Update current node (Left Half)
    // It keeps items [0 ... minNodeSize-1]
    // It keeps subtrees [0 ... minNodeSize]
    await ctx.db.patch(node, {
      items: newN.items.slice(0, minNodeSize),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(0, minNodeSize + 1)
        : [],
    });

    // 4b. Create new sibling node (Right Half)
    // It gets items [minNodeSize+1 ... end]
    // It gets subtrees [minNodeSize+1 ... end]
    const splitN = await ctx.db.insert("btreeNode", {
      items: newN.items.slice(minNodeSize + 1),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(minNodeSize + 1)
        : [],
    });

    // 4c. Return the PushUp object to the parent
    return {
      item: newN.items[minNodeSize] as Item, // The middle item acts as the separator
      leftSubtree: node,             // The current node (now smaller)
      rightSubtree: splitN,          // The new sibling node
    };
  }

  // No split needed
  return null;
}

function compareKeys(k1: Key, k2: Key) {
  return compareValues(k1, k2);
}

export async function getTree(db: DatabaseReader, namespace: Namespace) {
  return await db
    .query("btree")
    .withIndex("by_namespace", (q) => q.eq("namespace", namespace))
    .unique();
}

export async function mustGetTree(db: DatabaseReader, namespace: Namespace) {
  const tree = await getTree(db, namespace);
  if (!tree) {
    throw new Error("btree not initialized");
  }
  return tree;
}

export async function getOrCreateTree(
  db: DatabaseWriter,
  namespace: Namespace,
  maxNodeSize?: number,
): Promise<Doc<"btree">> {
  const originalTree = await getTree(db, namespace);
  if (originalTree) {
    return originalTree;
  }
  const root = await db.insert("btreeNode", {
    items: [],
    subtrees: [],
  });
  const effectiveMaxNodeSize =
    maxNodeSize ??
    (await MAX_NODE_SIZE({ db }, undefined)) ??
    DEFAULT_MAX_NODE_SIZE;
  const id = await db.insert("btree", {
    root,
    maxNodeSize: effectiveMaxNodeSize,
    namespace,
  });
  const newTree = await db.get(id);
  // Check the maxNodeSize is valid.
  await MIN_NODE_SIZE({ db }, namespace);
  return newTree!;
}

export const deleteTreeNodes = internalMutation({
  args: { node: v.id("btreeNode") },
  returns: v.null(),
  handler: async (ctx, { node }) => {
    const n = (await ctx.db.get(node))!;
    for (const subtree of n.subtrees) {
      await ctx.scheduler.runAfter(0, internal.btree.deleteTreeNodes, {
        node: subtree,
      });
    }
    await ctx.db.delete(node);
  },
});

export const paginate = query({
  args: {
    limit: v.number(),
    order: v.union(v.literal("asc"), v.literal("desc")),
    cursor: v.optional(v.string()),
    k1: v.optional(v.any()),
    k2: v.optional(v.any()),
    namespace: v.optional(v.any()),
  },
  returns: v.object({
    page: v.array(itemValidator),
    cursor: v.string(),
    isDone: v.boolean(),
  }),
  handler: paginateHandler,
});

/**
 * Handles pagination requests for browsing the B-tree.
 * Supports forward (asc) and backward (desc) iteration with cursors.
 */
export async function paginateHandler(
  ctx: { db: DatabaseReader },
  args: {
    limit: number;
    order: "asc" | "desc";
    cursor?: string;
    k1?: Key;
    k2?: Key;
    namespace?: Namespace;
  },
) {
  const tree = await getTree(ctx.db, args.namespace);
  if (tree === null) {
    return { page: [], cursor: "", isDone: true };
  }
  return await paginateInNode(
    ctx.db,
    tree.root,
    args.limit,
    args.order,
    args.cursor,
    args.k1,
    args.k2,
  );
}

/**
 * Recursively gathers a page of items from the B-tree.
 * Traverses subtrees in the correct order based on 'order'.
 */
export async function paginateInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  limit: number,
  order: "asc" | "desc",
  cursor?: string,
  k1?: Key,
  k2?: Key,
): Promise<{
  page: Item[];
  cursor: string;
  isDone: boolean;
}> {
  if (limit <= 0) {
    throw new ConvexError("limit must be positive");
  }
  if (cursor !== undefined && cursor.length === 0) {
    // Empty string is end cursor.
    return {
      page: [],
      cursor: "",
      isDone: true,
    };
  }
  const items: Item[] = [];
  const startKey =
    cursor === undefined || order === "desc"
      ? k1
      : jsonToConvex(JSON.parse(cursor));
  const endKey =
    cursor === undefined || order === "asc"
      ? k2
      : jsonToConvex(JSON.parse(cursor));

  // Find which parts of this node overlap with the requested range.
  const filtered = await filterBetween(db, node, startKey, endKey);

  if (order === "desc") {
    filtered.reverse();
  }

  // Collect items until we hit the limit.
  for (const included of filtered) {
    if (items.length >= limit) {
      // There's still more but the page is full.
      return {
        page: items,
        cursor: JSON.stringify(convexToJson(items[items.length - 1].k)),
        isDone: false,
      };
    }
    if (included.type === "item") {
      items.push(included.item);
    } else {
      // Recurse into subtree.
      const {
        page,
        cursor: newCursor,
        isDone,
      } = await paginateInNode(
        db,
        included.subtree,
        limit - items.length,
        order,
      );
      items.push(...page);
      if (!isDone) {
        return {
          page: items,
          cursor: newCursor,
          isDone: false,
        };
      }
    }
  }
  // If we finished the loop, we exhausted this node and its relevant subtrees.
  return {
    page: items,
    cursor: "",
    isDone: true,
  };
}

export const paginateNamespaces = query({
  args: {
    limit: v.number(),
    cursor: v.optional(v.string()),
  },
  returns: v.object({
    page: v.array(v.any()),
    cursor: v.string(),
    isDone: v.boolean(),
  }),
  handler: paginateNamespacesHandler,
});

export async function paginateNamespacesHandler(
  ctx: { db: DatabaseReader },
  args: { limit: number; cursor?: string },
) {
  if (args.cursor === "endcursor") {
    return {
      page: [],
      cursor: "endcursor",
      isDone: true,
    };
  }
  let trees = [];
  if (args.cursor === undefined) {
    trees = await ctx.db.query("btree").withIndex("by_id").take(args.limit);
  } else {
    trees = await ctx.db
      .query("btree")
      .withIndex("by_id", (q) => q.gt("_id", args.cursor as Id<"btree">))
      .take(args.limit);
  }
  const isDone = trees.length < args.limit;
  return {
    page: trees.map((t) => t.namespace ?? null),
    cursor: isDone ? "endcursor" : trees[trees.length - 1]._id,
    isDone,
  };
}
