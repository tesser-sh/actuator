import { Exit, Option, Result } from "effect";
import {
  FenceRefused,
  HelloRefused,
  MOVE_TAG,
  OPS,
  ORG_TAG,
  RateLimited,
  STAGE_TAG,
  type ActuatorConfig,
  type ActuatorHello,
  type ActuatorKind,
  type ActuatorLimits,
  type ActuatorOp,
  type AuditLine,
  type AwsCredentials,
  type HostedConfig,
  type InstanceQuery,
  type SelfHostConfig,
  type VcpuUsage,
  type VolumeQuery,
} from "./actuator-rpc.ts";
import { FLEET_HOST_TAG, WG_PORT } from "./fleet.ts";
import { MOVE_PORT_NBD, MOVE_PORT_VERDICT } from "./move.ts";
import { chunkKey } from "./store.ts";

export interface Filter {
  readonly Name: string;
  readonly Values: readonly string[];
}

const refuse = (fence: FenceRefused["fence"], message: string) => Result.fail(new FenceRefused({ fence, message }));

export function served(config: ActuatorConfig, op: ActuatorOp): Result.Result<void, FenceRefused> {
  const serves = OPS[op].serves;
  if (serves === "hosted" && config.kind !== "hosted") return refuse("hosted-only", `${op} runs only on tesser's own hosted actuators`);
  if (serves === "selfHost" && config.kind !== "selfHost") return refuse("self-host-only", `${op} runs only on a self-hosted org's actuator`);
  return Result.succeed(undefined);
}

export function actingOrg(config: ActuatorConfig, orgId: string | null): Result.Result<string | null, FenceRefused> {
  if (config.kind === "hosted" || orgId === config.orgId) return Result.succeed(orgId);
  return refuse("org", `this actuator serves ${config.orgId}, not ${orgId ?? "tesser's hosted fleet"}`);
}

const orgFilter = (orgId: string | null): Filter[] => (orgId === null ? [] : [{ Name: `tag:${ORG_TAG}`, Values: [orgId] }]);

export function instanceFilters(orgId: string | null, q: InstanceQuery): Filter[] {
  return [
    ...(q.instanceIds === undefined ? [] : [{ Name: "instance-id", Values: q.instanceIds }]),
    ...(q.tag === undefined ? [] : [q.tag.value === undefined ? { Name: "tag-key", Values: [q.tag.key] } : { Name: `tag:${q.tag.key}`, Values: [q.tag.value] }]),
    ...(q.stage === undefined ? [] : [{ Name: `tag:${STAGE_TAG}`, Values: [q.stage] }]),
    ...orgFilter(orgId),
  ];
}

export function volumeFilters(orgId: string, q: VolumeQuery): Filter[] {
  return [
    ...(q.volumeIds === undefined ? [] : [{ Name: "volume-id", Values: q.volumeIds }]),
    ...(q.attachedTo === undefined ? [] : [{ Name: "attachment.instance-id", Values: [q.attachedTo] }]),
    ...(q.device === undefined ? [] : [{ Name: "attachment.device", Values: [q.device] }]),
    ...(q.moving === undefined ? [] : [{ Name: "tag-key", Values: [MOVE_TAG] }]),
    ...orgFilter(orgId),
  ];
}

export function orgTags(orgId: string | null, tags: Readonly<Record<string, string>>): Record<string, string> {
  return { ...tags, ...(orgId === null ? {} : { [ORG_TAG]: orgId }) };
}

export function launchTags(config: ActuatorConfig, orgId: string | null, tags: Readonly<Record<string, string>>): Record<string, string> {
  return orgTags(orgId, { ...tags, ...(config.kind === "selfHost" ? config.tags : {}) });
}

export function launchAdmitted(limits: ActuatorLimits, live: number): Result.Result<void, FenceRefused> {
  if (live >= limits.maxInstances) return refuse("instances", `the org already runs ${live} instances, and this actuator caps it at ${limits.maxInstances}`);
  return Result.succeed(undefined);
}

export function chunkKeys(config: SelfHostConfig, ids: readonly string[]): string[] {
  return ids.map((id) => chunkKey(config.orgId, id));
}

export function boxUserData(config: ActuatorConfig, orgId: string, boxId: string): string {
  return JSON.stringify({ orgId, boxId, cellUrl: config.cellUrl });
}

export function hostUserData(
  config: ActuatorConfig,
  orgId: string | null,
  hostId: string,
  release: { hostd: { version: string; sha256: string }; image: { id: string; kernel: string; root: string } },
): string {
  const bucket = config.kind === "hosted" ? config.hosts.bucket : config.snapshots.bucket;
  const imagesBucket = config.kind === "hosted" ? config.images?.buckets[config.region] : undefined;
  const reads = orgId !== null ? { orgId } : imagesBucket !== undefined ? { imagesBucket } : {};
  return JSON.stringify({ hostId, cellUrl: config.cellUrl, bucket, ...reads, hostd: release.hostd, image: release.image });
}

interface IngressRule {
  readonly protocol: "tcp" | "udp";
  readonly fromPort: number;
  readonly toPort: number;
  readonly from: { readonly cidr: string } | { readonly groupId: string };
}

export function orgGroup(config: HostedConfig, orgId: string): { name: string; tags: Record<string, string>; rules: (groupId: string) => IngressRule[] } {
  const name = `tesser-${orgId}`;
  const hosts = config.securityGroupId;
  return {
    name,
    tags: { Name: name, "tesser:managed": "true", [ORG_TAG]: orgId },
    rules: (groupId) => [
      { protocol: "tcp", fromPort: 22, toPort: 22, from: { cidr: "0.0.0.0/0" } },
      { protocol: "udp", fromPort: WG_PORT, toPort: WG_PORT, from: { groupId } },
      { protocol: "tcp", fromPort: MOVE_PORT_NBD, toPort: MOVE_PORT_VERDICT, from: { groupId: hosts } },
      { protocol: "tcp", fromPort: MOVE_PORT_NBD, toPort: MOVE_PORT_VERDICT, from: { groupId } },
      { protocol: "udp", fromPort: WG_PORT, toPort: WG_PORT, from: { groupId: hosts } },
    ],
  };
}

const ORG_SESSION_TAG = "org";
const CREDENTIALS_TTL_S = 3600;

export function orgS3Session(config: HostedConfig, orgId: string): { RoleArn: string; RoleSessionName: string; DurationSeconds: number; Tags: { Key: string; Value: string }[] } {
  return { RoleArn: config.snapshots.orgRoleArn, RoleSessionName: orgId, DurationSeconds: CREDENTIALS_TTL_S, Tags: [{ Key: ORG_SESSION_TAG, Value: orgId }] };
}

export function imagesSession(
  config: HostedConfig,
  region: string,
  hostId: string,
  keys: readonly string[],
): Result.Result<{ RoleArn: string; RoleSessionName: string; DurationSeconds: number; Policy: string }, FenceRefused> {
  const bucket = config.images?.buckets[region];
  if (config.images === null || bucket === undefined) return refuse("not-configured", `this hosted actuator has no images bucket in ${region}`);
  const policy = { Version: "2012-10-17", Statement: [{ Effect: "Allow", Action: "s3:GetObject", Resource: keys.map((key) => `arn:aws:s3:::${bucket}/${key}`) }] };
  return Result.succeed({ RoleArn: config.images.readerRoleArn, RoleSessionName: hostId, DurationSeconds: 15 * 60, Policy: JSON.stringify(policy) });
}

export interface ActuatorSeat {
  readonly kind: ActuatorKind;
  readonly accountId: string;
  readonly region: string;
  readonly roleArn: string;
  readonly orgId: string | null;
}

export interface VerifiedIdentity {
  readonly accountId: string;
  readonly principalArn: string;
  readonly audience: string;
}

export function seated(seat: ActuatorSeat, hello: ActuatorHello, identity: VerifiedIdentity): Result.Result<void, HelloRefused> {
  const config = hello.config;
  const no = (message: string) => Result.fail(new HelloRefused({ message }));
  if (config.kind !== seat.kind) return no(`this seat takes a ${seat.kind} actuator, and this one is ${config.kind}`);
  if (identity.accountId !== seat.accountId || config.accountId !== seat.accountId)
    return no(`this seat is account ${seat.accountId}; the token says ${identity.accountId} and the config ${config.accountId}`);
  if (config.region !== seat.region) return no(`this seat is ${seat.region}, and the config says ${config.region}`);
  if (identity.principalArn !== seat.roleArn) return no(`this seat takes ${seat.roleArn}, and the token is ${identity.principalArn}`);
  if (identity.audience !== config.cellUrl) return no(`the token is for ${identity.audience}, not ${config.cellUrl}`);
  if (config.kind === "selfHost" && config.orgId !== seat.orgId) return no(`this seat serves ${seat.orgId ?? "tesser's hosted orgs"}, and the config says ${config.orgId}`);
  return Result.succeed(undefined);
}

interface ActuatorLink {
  readonly version: string;
  readonly ops: readonly string[];
  readonly draining: boolean;
}

const semver = (v: string) => v.split(".").map(Number);
const newer = (a: string, b: string) => {
  const [x, y] = [semver(a), semver(b)];
  const i = x.findIndex((n, k) => n !== y[k]);
  return i === -1 ? 0 : x[i]! - y[i]!;
};

export function route<L extends ActuatorLink>(links: readonly L[], op: ActuatorOp): L | null {
  const live = links.filter((l) => !l.draining && l.ops.includes(op));
  return live.reduce<L | null>((best, l) => (best === null || newer(l.version, best.version) > 0 ? l : best), null);
}

const json = (value: unknown) => JSON.stringify(value) ?? "null";
const refusalOf = (e: { readonly _tag: string; readonly message?: string | undefined }) => ({ _tag: e._tag, ...(e.message === undefined ? {} : { message: e.message }) });

export function auditLine(
  op: ActuatorOp,
  at: number,
  orgId: string | null,
  request: unknown,
  exit: Exit.Exit<unknown, { readonly _tag: string; readonly message?: string | undefined }>,
): AuditLine | null {
  const line = { at, op, orgId, request: json(request) };
  if (Exit.isFailure(exit)) {
    const refused = Option.match(Exit.findErrorOption(exit), { onNone: () => ({ _tag: "Defect" }), onSome: refusalOf });
    if (OPS[op].audit === "read") return { _tag: "Refused", ...line, refused };
    if (OPS[op].audit === "grant") return { _tag: "Grant", ...line, outcome: { refused } };
    return { _tag: "Mutation", ...line, outcome: { refused } };
  }
  if (OPS[op].audit === "read") return null;
  if (OPS[op].audit === "grant") {
    const granted = exit.value as AwsCredentials;
    return { _tag: "Grant", ...line, outcome: { accessKeyId: granted.accessKeyId, expiration: granted.expiration } };
  }
  return { _tag: "Mutation", ...line, outcome: { ok: json(exit.value) } };
}

export function admit(times: readonly number[], now: number, limit: number, windowMs: number): Result.Result<number[], RateLimited> {
  const kept = times.filter((t) => t > now - windowMs);
  if (kept.length < limit) return Result.succeed([...kept, now]);
  return Result.fail(new RateLimited({ retryAfterMs: Math.max(1, kept[kept.length - limit]! + windowMs - now) }));
}

const STANDARD_FAMILY = /^(?:a|c|d|h|i|m|r|t|z|im|is)\d/;
const UP = new Set(["pending", "running"]);

export const stateReason = (r: { readonly Code?: string; readonly Message?: string } | undefined): string | null => {
  const code = r?.Code || null;
  const message = r?.Message ?? null;
  if (message === null || message === "") return code;
  return (code === null || message.startsWith(code) ? message : `${code}: ${message}`).slice(0, 512);
};

export function vcpuUsage(orgId: string | null, instances: readonly { state: string; instanceType: string; vcpus: number; spot: boolean; tags: Readonly<Record<string, string>> }[]): VcpuUsage {
  const onDemand = instances.filter((i) => !i.spot);
  const standard = onDemand.filter((i) => STANDARD_FAMILY.test(i.instanceType));
  const sum = (list: typeof instances) => list.reduce((n, i) => n + i.vcpus, 0);
  return {
    onDemand: sum(onDemand),
    standardOnDemand: sum(standard),
    orgHosts: sum(standard.filter((i) => i.tags[FLEET_HOST_TAG] !== undefined && (i.tags[ORG_TAG] ?? null) === orgId && UP.has(i.state))),
  };
}
