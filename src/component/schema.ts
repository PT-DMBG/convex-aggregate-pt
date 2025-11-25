import { defineSchema, defineTable } from "convex/server";
import { type Value as ConvexValue, v, type GenericId } from "convex/values";

const item = v.object({
  // key, usually an index key.
  k: v.any(),
  // value, usually an id.
  v: v.string(),
});

export type Item = {
  k: ConvexValue;
  v: GenericId<string>;
};

export const itemValidator = v.object({
  k: v.any(),
  v: v.string(),
});

export default defineSchema({
  // One per namespace
  btree: defineTable({
    root: v.id("btreeNode"),
    namespace: v.optional(v.any()),
    maxNodeSize: v.number(),
  }).index("by_namespace", ["namespace"]),
  btreeNode: defineTable({
    items: v.array(item),
    subtrees: v.array(v.id("btreeNode")),
  }),
});
