import type {
  DocumentByName,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  TableNamesInDataModel,
} from "convex/server";
import type { Key } from "../component/btree.js";
import {
  type Position,
  positionToKey,
  keyToPosition,
  type Bound,
  type Bounds,
  boundsToPositions,
} from "./positions.js";
import type { GenericId, Value as ConvexValue } from "convex/values";
import type { ComponentApi } from "../component/_generated/component.js";

// e.g. `ctx` from a Convex query or mutation or action.
export type RunQueryCtx = {
  runQuery: GenericQueryCtx<GenericDataModel>["runQuery"];
};

// e.g. `ctx` from a Convex mutation or action.
export type RunMutationCtx = {
  runMutation: GenericMutationCtx<GenericDataModel>["runMutation"];
};

export type Item<K extends Key, ID extends string> = {
  key: K;
  id: ID;
};

export type { Key, Bound };

/**
 * Write data to be indexed, and read indexed data.
 *
 * The data structure is effectively a key-value store sorted by key, where the
 * value is an ID.
 * 1. The key can be any Convex value (number, string, array, etc.).
 * 2. The ID is a string which should be unique.
 *
 * Once values have been added to the data structure, you can query for items
 * between a range of keys.
 */
export class SearchTree<
  K extends Key,
  ID extends string,
  Namespace extends ConvexValue | undefined = undefined,
> {
  constructor(protected component: ComponentApi) { }

  /// Queries.

  /**
   * Gets the minimum item within the given bounds.
   */
  async min(
    ctx: RunQueryCtx,
    ...opts: NamespacedOpts<{ bounds?: Bounds<K, ID> }, Namespace>
  ): Promise<Item<K, ID> | null> {
    const { page } = await this.paginate(ctx, {
      namespace: namespaceFromOpts(opts),
      bounds: opts[0]?.bounds,
      order: "asc",
      pageSize: 1,
    });
    return page[0] ?? null;
  }
  /**
   * Gets the maximum item within the given bounds.
   */
  async max(
    ctx: RunQueryCtx,
    ...opts: NamespacedOpts<{ bounds?: Bounds<K, ID> }, Namespace>
  ): Promise<Item<K, ID> | null> {
    const { page } = await this.paginate(ctx, {
      namespace: namespaceFromOpts(opts),
      bounds: opts[0]?.bounds,
      order: "desc",
      pageSize: 1,
    });
    return page[0] ?? null;
  }
  /**
   * Get a page of items between the given bounds, with a cursor to paginate.
   * Use `iter` to iterate over all items within the bounds.
   */
  async paginate(
    ctx: RunQueryCtx,
    ...opts: NamespacedOpts<
      {
        bounds?: Bounds<K, ID>;
        cursor?: string;
        order?: "asc" | "desc";
        pageSize?: number;
      },
      Namespace
    >
  ): Promise<{ page: Item<K, ID>[]; cursor: string; isDone: boolean }> {
    const order = opts[0]?.order ?? "asc";
    const pageSize = opts[0]?.pageSize ?? 100;
    const {
      page,
      cursor: newCursor,
      isDone,
    } = await ctx.runQuery(this.component.btree.paginate, {
      namespace: namespaceFromOpts(opts),
      ...boundsToPositions(opts[0]?.bounds),
      cursor: opts[0]?.cursor,
      order,
      limit: pageSize,
    });
    return {
      page: page.map(btreeItemToAggregateItem<K, ID>),
      cursor: newCursor,
      isDone,
    };
  }
  /**
   * Example usage:
   * ```ts
   * for await (const item of tree.iter(ctx, bounds)) {
   *   console.log(item);
   * }
   * ```
   */
  async *iter(
    ctx: RunQueryCtx,
    ...opts: NamespacedOpts<
      { bounds?: Bounds<K, ID>; order?: "asc" | "desc"; pageSize?: number },
      Namespace
    >
  ): AsyncGenerator<Item<K, ID>, void, undefined> {
    const order = opts[0]?.order ?? "asc";
    const pageSize = opts[0]?.pageSize ?? 100;
    const bounds = opts[0]?.bounds;
    const namespace = namespaceFromOpts(opts);
    let isDone = false;
    let cursor: string | undefined = undefined;
    while (!isDone) {
      const {
        page,
        cursor: newCursor,
        isDone: newIsDone,
      } = await this.paginate(ctx, {
        namespace,
        bounds,
        cursor,
        order,
        pageSize,
      });
      for (const item of page) {
        yield item;
      }
      isDone = newIsDone;
      cursor = newCursor;
    }
  }

  /** Write operations. See {@link DirectSearchTree} for docstrings. */
  async _insert(
    ctx: RunMutationCtx,
    namespace: Namespace,
    key: K,
    id: ID,
  ): Promise<void> {
    await ctx.runMutation(this.component.public.insert, {
      key: keyToPosition(key, id),
      value: id,
      namespace,
    });
  }
  async _delete(
    ctx: RunMutationCtx,
    namespace: Namespace,
    key: K,
    id: ID,
  ): Promise<void> {
    await ctx.runMutation(this.component.public.delete_, {
      key: keyToPosition(key, id),
      namespace,
    });
  }
  async _replace(
    ctx: RunMutationCtx,
    currentNamespace: Namespace,
    currentKey: K,
    newNamespace: Namespace,
    newKey: K,
    id: ID,
  ): Promise<void> {
    await ctx.runMutation(this.component.public.replace, {
      currentKey: keyToPosition(currentKey, id),
      newKey: keyToPosition(newKey, id),
      value: id,
      namespace: currentNamespace,
      newNamespace,
    });
  }
  async _insertIfDoesNotExist(
    ctx: RunMutationCtx,
    namespace: Namespace,
    key: K,
    id: ID,
  ): Promise<void> {
    await this._replaceOrInsert(
      ctx,
      namespace,
      key,
      namespace,
      key,
      id,
    );
  }
  async _deleteIfExists(
    ctx: RunMutationCtx,
    namespace: Namespace,
    key: K,
    id: ID,
  ): Promise<void> {
    await ctx.runMutation(this.component.public.deleteIfExists, {
      key: keyToPosition(key, id),
      namespace,
    });
  }
  async _replaceOrInsert(
    ctx: RunMutationCtx,
    currentNamespace: Namespace,
    currentKey: K,
    newNamespace: Namespace,
    newKey: K,
    id: ID,
  ): Promise<void> {
    await ctx.runMutation(this.component.public.replaceOrInsert, {
      currentKey: keyToPosition(currentKey, id),
      newKey: keyToPosition(newKey, id),
      value: id,
      namespace: currentNamespace,
      newNamespace,
    });
  }

  /// Initialization and maintenance.

  /**
   * (re-)initialize the data structure, removing all items if it exists.
   *
   * Change the maxNodeSize if provided, otherwise keep it the same.
   *   maxNodeSize is how you tune the data structure's width and depth.
   *   Larger values can reduce write contention but increase read latency.
   *   Default is 16.
   */
  async clear(
    ctx: RunMutationCtx,
    ...opts: NamespacedOpts<
      { maxNodeSize?: number },
      Namespace
    >
  ): Promise<void> {
    await ctx.runMutation(this.component.public.clear, {
      maxNodeSize: opts[0]?.maxNodeSize,
      namespace: namespaceFromOpts(opts),
    });
  }

  async paginateNamespaces(
    ctx: RunQueryCtx,
    cursor?: string,
    pageSize: number = 100,
  ): Promise<{ page: Namespace[]; cursor: string; isDone: boolean }> {
    const {
      page,
      cursor: newCursor,
      isDone,
    } = await ctx.runQuery(this.component.btree.paginateNamespaces, {
      cursor,
      limit: pageSize,
    });
    return {
      page: page as Namespace[],
      cursor: newCursor,
      isDone,
    };
  }

  async *iterNamespaces(
    ctx: RunQueryCtx,
    pageSize: number = 100,
  ): AsyncGenerator<Namespace, void, undefined> {
    let isDone = false;
    let cursor: string | undefined = undefined;
    while (!isDone) {
      const {
        page,
        cursor: newCursor,
        isDone: newIsDone,
      } = await this.paginateNamespaces(ctx, cursor, pageSize);
      for (const item of page) {
        yield item ?? (undefined as Namespace);
      }
      isDone = newIsDone;
      cursor = newCursor;
    }
  }

  async clearAll(
    ctx: RunMutationCtx & RunQueryCtx,
    opts?: { maxNodeSize?: number },
  ): Promise<void> {
    for await (const namespace of this.iterNamespaces(ctx)) {
      await this.clear(ctx, { ...opts, namespace });
    }
    // In case there are no namespaces, make sure we create at least one tree,
    // at namespace=undefined. This is where the default settings are stored.
    await this.clear(ctx, { ...opts, namespace: undefined as Namespace });
  }
}

export type DirectSearchTreeType<
  K extends Key,
  ID extends string,
  Namespace extends ConvexValue | undefined = undefined,
> = {
  Key: K;
  Id: ID;
  Namespace?: Namespace;
};
type AnyDirectSearchTreeType = DirectSearchTreeType<
  Key,
  string,
  ConvexValue | undefined
>;
type DirectSearchTreeNamespace<T extends AnyDirectSearchTreeType> =
  "Namespace" extends keyof T ? T["Namespace"] : undefined;

/**
 * A DirectSearchTree is a SearchTree where you can insert, delete, and replace
 * items directly, and keys and IDs can be customized.
 *
 * Contrast with TableSearchTree, which follows a table with Triggers and
 * computes keys from the table's documents.
 */
export class DirectSearchTree<
  T extends AnyDirectSearchTreeType,
> extends SearchTree<T["Key"], T["Id"], DirectSearchTreeNamespace<T>> {
  /**
   * Insert a new key into the data structure.
   * The id should be unique.
   * If the tree does not exist yet, it will be initialized with the default
   * maxNodeSize.
   * If the [key, id] pair already exists, this will throw.
   */
  async insert(
    ctx: RunMutationCtx,
    args: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._insert(
      ctx,
      namespaceFromArg(args),
      args.key,
      args.id,
    );
  }
  /**
   * Delete the key with the given ID from the data structure.
   * Throws if the given key and ID do not exist.
   */
  async delete(
    ctx: RunMutationCtx,
    args: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._delete(ctx, namespaceFromArg(args), args.key, args.id);
  }
  /**
   * Update an existing item in the data structure.
   * This is effectively a delete followed by an insert, but it's performed
   * atomically so it's impossible to view the data structure with the key missing.
   */
  async replace(
    ctx: RunMutationCtx,
    currentItem: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
    newItem: NamespacedArgs<
      { key: T["Key"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._replace(
      ctx,
      namespaceFromArg(currentItem),
      currentItem.key,
      namespaceFromArg(newItem),
      newItem.key,
      currentItem.id,
    );
  }
  /**
   * Equivalents to `insert`, `delete`, and `replace` where the item may or may not exist.
   * This can be useful for live backfills:
   * 1. Update live writes to use these methods to write into the new SearchTree.
   * 2. Run a background backfill, paginating over existing data, calling `insertIfDoesNotExist` on each item.
   * 3. Once the backfill is complete, use `insert`, `delete`, and `replace` for live writes.
   * 4. Begin using the SearchTree read methods.
   */
  async insertIfDoesNotExist(
    ctx: RunMutationCtx,
    args: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._insertIfDoesNotExist(
      ctx,
      namespaceFromArg(args),
      args.key,
      args.id,
    );
  }
  async deleteIfExists(
    ctx: RunMutationCtx,
    args: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._deleteIfExists(ctx, namespaceFromArg(args), args.key, args.id);
  }
  async replaceOrInsert(
    ctx: RunMutationCtx,
    currentItem: NamespacedArgs<
      { key: T["Key"]; id: T["Id"] },
      DirectSearchTreeNamespace<T>
    >,
    newItem: NamespacedArgs<
      { key: T["Key"] },
      DirectSearchTreeNamespace<T>
    >,
  ): Promise<void> {
    await this._replaceOrInsert(
      ctx,
      namespaceFromArg(currentItem),
      currentItem.key,
      namespaceFromArg(newItem),
      newItem.key,
      currentItem.id,
    );
  }
}

export type TableSearchTreeType<
  K extends Key,
  DataModel extends GenericDataModel,
  TableName extends TableNamesInDataModel<DataModel>,
  Namespace extends ConvexValue | undefined = undefined,
> = {
  Key: K;
  DataModel: DataModel;
  TableName: TableName;
  Namespace?: Namespace;
};

type AnyTableSearchTreeType = TableSearchTreeType<
  Key,
  GenericDataModel,
  TableNamesInDataModel<GenericDataModel>,
  ConvexValue | undefined
>;
type TableSearchTreeNamespace<T extends AnyTableSearchTreeType> =
  "Namespace" extends keyof T ? T["Namespace"] : undefined;
type TableSearchTreeDocument<T extends AnyTableSearchTreeType> = DocumentByName<
  T["DataModel"],
  T["TableName"]
>;
type TableSearchTreeId<T extends AnyTableSearchTreeType> = GenericId<
  T["TableName"]
>;
type TableSearchTreeTrigger<Ctx, T extends AnyTableSearchTreeType> = Trigger<
  Ctx,
  T["DataModel"],
  T["TableName"]
>;

export class TableSearchTree<T extends AnyTableSearchTreeType> extends SearchTree<
  T["Key"],
  GenericId<T["TableName"]>,
  TableSearchTreeNamespace<T>
> {
  constructor(
    component: ComponentApi,
    private options: {
      sortKey: (d: TableSearchTreeDocument<T>) => T["Key"];
    } & (undefined extends TableSearchTreeNamespace<T>
      ? {
        namespace?: (
          d: TableSearchTreeDocument<T>,
        ) => TableSearchTreeNamespace<T>;
      }
      : {
        namespace: (
          d: TableSearchTreeDocument<T>,
        ) => TableSearchTreeNamespace<T>;
      }),
  ) {
    super(component);
  }

  async insert(
    ctx: RunMutationCtx,
    doc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._insert(
      ctx,
      this.options.namespace?.(doc),
      this.options.sortKey(doc),
      doc._id as TableSearchTreeId<T>,
    );
  }
  async delete(
    ctx: RunMutationCtx,
    doc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._delete(
      ctx,
      this.options.namespace?.(doc),
      this.options.sortKey(doc),
      doc._id as TableSearchTreeId<T>,
    );
  }
  async replace(
    ctx: RunMutationCtx,
    oldDoc: TableSearchTreeDocument<T>,
    newDoc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._replace(
      ctx,
      this.options.namespace?.(oldDoc),
      this.options.sortKey(oldDoc),
      this.options.namespace?.(newDoc),
      this.options.sortKey(newDoc),
      newDoc._id as TableSearchTreeId<T>,
    );
  }
  async insertIfDoesNotExist(
    ctx: RunMutationCtx,
    doc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._insertIfDoesNotExist(
      ctx,
      this.options.namespace?.(doc),
      this.options.sortKey(doc),
      doc._id as TableSearchTreeId<T>,
    );
  }
  async deleteIfExists(
    ctx: RunMutationCtx,
    doc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._deleteIfExists(
      ctx,
      this.options.namespace?.(doc),
      this.options.sortKey(doc),
      doc._id as TableSearchTreeId<T>,
    );
  }
  async replaceOrInsert(
    ctx: RunMutationCtx,
    oldDoc: TableSearchTreeDocument<T>,
    newDoc: TableSearchTreeDocument<T>,
  ): Promise<void> {
    await this._replaceOrInsert(
      ctx,
      this.options.namespace?.(oldDoc),
      this.options.sortKey(oldDoc),
      this.options.namespace?.(newDoc),
      this.options.sortKey(newDoc),
      newDoc._id as TableSearchTreeId<T>,
    );
  }

  trigger<Ctx extends RunMutationCtx>(): TableSearchTreeTrigger<Ctx, T> {
    return async (ctx, change) => {
      if (change.operation === "insert") {
        await this.insert(ctx, change.newDoc);
      } else if (change.operation === "update") {
        await this.replace(ctx, change.oldDoc, change.newDoc);
      } else if (change.operation === "delete") {
        await this.delete(ctx, change.oldDoc);
      }
    };
  }

  idempotentTrigger<Ctx extends RunMutationCtx>(): TableSearchTreeTrigger<
    Ctx,
    T
  > {
    return async (ctx, change) => {
      if (change.operation === "insert") {
        await this.insertIfDoesNotExist(ctx, change.newDoc);
      } else if (change.operation === "update") {
        await this.replaceOrInsert(ctx, change.oldDoc, change.newDoc);
      } else if (change.operation === "delete") {
        await this.deleteIfExists(ctx, change.oldDoc);
      }
    };
  }
}

export type Trigger<
  Ctx,
  DataModel extends GenericDataModel,
  TableName extends TableNamesInDataModel<DataModel>,
> = (ctx: Ctx, change: Change<DataModel, TableName>) => Promise<void>;

export type Change<
  DataModel extends GenericDataModel,
  TableName extends TableNamesInDataModel<DataModel>,
> = {
  id: GenericId<TableName>;
} & (
    | {
      operation: "insert";
      oldDoc: null;
      newDoc: DocumentByName<DataModel, TableName>;
    }
    | {
      operation: "update";
      oldDoc: DocumentByName<DataModel, TableName>;
      newDoc: DocumentByName<DataModel, TableName>;
    }
    | {
      operation: "delete";
      oldDoc: DocumentByName<DataModel, TableName>;
      newDoc: null;
    }
  );

export function btreeItemToAggregateItem<K extends Key, ID extends string>({
  k,
}: {
  k: unknown;
}): Item<K, ID> {
  const { key, id } = positionToKey(k as Position);
  return {
    key: key as K,
    id: id as ID,
  };
}

export type NamespacedArgs<Args, Namespace> =
  | (Args & { namespace: Namespace })
  | (Namespace extends undefined ? Args : never);

export type NamespacedOpts<Opts, Namespace> =
  | [{ namespace: Namespace } & Opts]
  | (undefined extends Namespace ? [Opts?] : never);

export type NamespacedOptsBatch<Opts, Namespace> = Array<
  undefined extends Namespace ? Opts : { namespace: Namespace } & Opts
>;

function namespaceFromArg<Namespace>(
  args: { namespace: Namespace } | object,
): Namespace {
  if ("namespace" in args) {
    return args["namespace"]!;
  }
  return undefined as Namespace;
}
function namespaceFromOpts<Opts, Namespace>(
  opts: NamespacedOpts<Opts, Namespace>,
): Namespace {
  if (opts.length === 0) {
    // Only possible if Namespace extends undefined, so undefined is the only valid namespace.
    return undefined as Namespace;
  }
  const [{ namespace }] = opts as [{ namespace: Namespace }];
  return namespace;
}
