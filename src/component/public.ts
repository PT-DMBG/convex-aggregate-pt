import { ConvexError, v } from "convex/values";
import { mutation } from "./_generated/server.js";
import {
  DEFAULT_MAX_NODE_SIZE,
  deleteHandler,
  getOrCreateTree,
  getTree,
  insertHandler,
  type Value,
} from "./btree.js";
import { internal } from "./_generated/api.js";

export const init = mutation({
  args: {
    maxNodeSize: v.optional(v.number()),
    namespace: v.optional(v.any()),
  },
  returns: v.null(),
  handler: async (ctx, { maxNodeSize, namespace }) => {
    const existing = await getTree(ctx.db, namespace);
    if (existing) {
      throw new Error("tree already initialized");
    }
    await getOrCreateTree(
      ctx.db,
      namespace,
      maxNodeSize ?? DEFAULT_MAX_NODE_SIZE,
    );
  },
});

export const insert = mutation({
  args: {
    key: v.any(),
    value: v.string(),
    namespace: v.optional(v.any()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await insertHandler(ctx, { ...args, value: args.value as Value });
  },
});

// delete is a keyword, hence the underscore.
export const delete_ = mutation({
  args: { key: v.any(), namespace: v.optional(v.any()) },
  returns: v.null(),
  handler: deleteHandler,
});

export const replace = mutation({
  args: {
    currentKey: v.any(),
    newKey: v.any(),
    value: v.string(),
    namespace: v.optional(v.any()),
    newNamespace: v.optional(v.any()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await deleteHandler(ctx, {
      key: args.currentKey,
      namespace: args.namespace,
    });
    await insertHandler(ctx, {
      key: args.newKey,
      value: args.value as Value,
      namespace: args.newNamespace,
    });
  },
});

export const deleteIfExists = mutation({
  args: { key: v.any(), namespace: v.optional(v.any()) },
  handler: async (ctx, { key, namespace }) => {
    try {
      await deleteHandler(ctx, { key, namespace });
    } catch (e) {
      if (e instanceof ConvexError && e.data?.code === "DELETE_MISSING_KEY") {
        return;
      }
      throw e;
    }
  },
});

export const replaceOrInsert = mutation({
  args: {
    currentKey: v.any(),
    newKey: v.any(),
    value: v.string(),
    namespace: v.optional(v.any()),
    newNamespace: v.optional(v.any()),
  },
  handler: async (ctx, args) => {
    try {
      await deleteHandler(ctx, {
        key: args.currentKey,
        namespace: args.namespace,
      });
    } catch (e) {
      if (
        !(e instanceof ConvexError && e.data?.code === "DELETE_MISSING_KEY")
      ) {
        throw e;
      }
    }
    await insertHandler(ctx, {
      key: args.newKey,
      value: args.value as Value,
      namespace: args.newNamespace,
    });
  },
});

/**
 * Reinitialize the aggregate data structure, clearing all data.
 * maxNodeSize is the sharding coefficient for the underlying btree.
 * If not provided, the existing value is preserved.
 */
export const clear = mutation({
  args: {
    namespace: v.optional(v.any()),
    maxNodeSize: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, { maxNodeSize, namespace }) => {
    const tree = await getTree(ctx.db, namespace);
    let existingMaxNodeSize = DEFAULT_MAX_NODE_SIZE;
    if (tree) {
      await ctx.db.delete(tree._id);
      existingMaxNodeSize = tree.maxNodeSize;
      await ctx.scheduler.runAfter(0, internal.btree.deleteTreeNodes, {
        node: tree.root,
      });
    }
    await getOrCreateTree(
      ctx.db,
      namespace,
      maxNodeSize ?? existingMaxNodeSize,
    );
  },
});
