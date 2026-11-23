import { Schema } from "effect";

export const CHUNK_BYTES = 2 * 1024 ** 2;
const STREAM_MAX_BYTES = 1024 ** 4;
export const MANIFEST_MAX_BYTES = 4 * 1024 ** 2;
const MAX_STREAMS = 8;
export const STORE_BATCH = 1024;

const pattern = (re: RegExp) => Schema.String.check(Schema.isPattern(re));
const Index = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: STREAM_MAX_BYTES / CHUNK_BYTES - 1 }));

const ChunkId = pattern(/^[0-9a-f]{64}$/);
const ManifestId = ChunkId;
const RefName = pattern(/^(box\/box|snap\/snap)_[0-9a-f]{16}$/);
const ImageId = pattern(/^[a-z0-9-]{1,64}$/);
const StreamName = pattern(/^[a-z][a-z0-9-]{0,31}$/);

const DiskStream = Schema.Struct({
  bytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: STREAM_MAX_BYTES })),
  chunks: Schema.Array(Schema.Tuple([Index, ChunkId])),
  hot: Schema.Array(Index),
});
export type DiskStream = typeof DiskStream.Type;

export const DiskManifest = Schema.Struct({
  parent: Schema.NullOr(ManifestId),
  image: Schema.NullOr(ImageId),
  streams: Schema.Record(StreamName, DiskStream),
});
export type DiskManifest = typeof DiskManifest.Type;

const increasing = (xs: readonly number[]) => xs.every((x, i) => i === 0 || x > xs[i - 1]!);

export function diskManifestProblem(m: DiskManifest): string | null {
  const names = Object.keys(m.streams);
  if (names.length === 0 || names.length > MAX_STREAMS) return `a manifest holds 1-${MAX_STREAMS} streams, not ${names.length}`;
  for (const name of names) {
    const s = m.streams[name]!;
    const indexes = s.chunks.map(([i]) => i);
    if (!increasing(indexes)) return `stream ${name}: chunk indexes must strictly increase`;
    const last = indexes.at(-1);
    if (last !== undefined && last * CHUNK_BYTES >= s.bytes) return `stream ${name}: chunk ${last} starts past its ${s.bytes} bytes`;
    if (!increasing(s.hot)) return `stream ${name}: hot indexes must strictly increase`;
    const held = new Set(indexes);
    const cold = s.hot.find((i) => !held.has(i));
    if (cold !== undefined) return `stream ${name}: hot chunk ${cold} is not one of its chunks`;
  }
  return null;
}

export function encodeDiskManifest(m: DiskManifest): string {
  const streams = Object.fromEntries(
    Object.keys(m.streams)
      .sort()
      .map((name) => {
        const { bytes, chunks, hot } = m.streams[name]!;
        return [name, { bytes, chunks, hot }];
      }),
  );
  return JSON.stringify({ parent: m.parent, image: m.image, streams });
}

export async function diskManifestId(m: DiskManifest): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(encodeDiskManifest(m)));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

export const chunkPrefix = (orgId: string) => `${orgId}/c/`;
export const chunkKey = (orgId: string, id: string) => `${chunkPrefix(orgId)}${id.slice(0, 2)}/${id}`;

export const ChunkAccess = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("signed"), urls: Schema.Record(ChunkId, Schema.String), expiresAt: Schema.Number }),
  Schema.Struct({ kind: Schema.Literal("role"), bucket: Schema.String, region: Schema.String, prefix: Schema.String }),
]);
export type ChunkAccess = typeof ChunkAccess.Type;

const Batch = Schema.Array(ChunkId).check(Schema.isMinLength(1), Schema.isMaxLength(STORE_BATCH));

export const StoreHave = Schema.Struct({ ids: Batch });
export const StoreHaveAnswer = Schema.Struct({ at: Schema.Number, absent: Schema.Array(ChunkId), wait: Schema.Array(ChunkId), put: Schema.NullOr(ChunkAccess) });
export type StoreHaveAnswer = typeof StoreHaveAnswer.Type;

export const StoreCommit = Schema.Struct({ ref: RefName, id: ManifestId, expect: Schema.NullOr(ManifestId), since: Schema.Number, manifest: DiskManifest });
export const StoreCommitAnswer = Schema.Union([Schema.Struct({ committed: ManifestId }), Schema.Struct({ missing: Schema.Array(ChunkId) }), Schema.Struct({ conflict: Schema.NullOr(ManifestId) })]);
export type StoreCommitAnswer = typeof StoreCommitAnswer.Type;

export const StoreRead = Schema.Struct({ manifest: ManifestId, ids: Batch });

export const StoreRefAnswer = Schema.Struct({ ref: RefName, id: ManifestId, manifest: DiskManifest });
export type StoreRefAnswer = typeof StoreRefAnswer.Type;

export const StoreListing = Schema.Struct({
  refs: Schema.Array(Schema.Struct({ name: Schema.String, id: Schema.String, image: Schema.NullOr(Schema.String), bytes: Schema.Number, chunks: Schema.Number, at: Schema.String })),
  images: Schema.Array(Schema.Struct({ id: Schema.String, manifests: Schema.Number })),
  chunks: Schema.Struct({ pending: Schema.Number, live: Schema.Number, orphan: Schema.Number, deleting: Schema.Number }),
}).annotate({ identifier: "StoreListing" });
export type StoreListing = typeof StoreListing.Type;
