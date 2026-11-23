import { AccessDeniedException, UnknownAwsError } from "@distilled.cloud/aws/Errors";
import { IncorrectInstanceState, InsufficientInstanceCapacity, InvalidInstanceIDNotFound, RequestLimitExceeded, UnauthorizedOperation, Unsupported } from "@distilled.cloud/aws/ec2";
import { AccessDenied } from "@distilled.cloud/aws/s3";
import { Schema, SchemaTransformation } from "effect";
import type { YieldableError } from "effect/Cause";
import { Rpc, RpcGroup } from "effect/rpc";
import { cloudTagsProblem } from "./aws.ts";
import { AWS_REGION_RE } from "./env.ts";
import { FLEET_HOST_TAG, HOST_ID, HostdBuild, MAX_FLEET_FILES } from "./fleet.ts";
import { BOX_ID_PATTERN, ORG_ID_PATTERN } from "./org.ts";

export const MOVE_TAG = "tesser:move";
export const ORG_TAG = "tesser:org";
export const STAGE_TAG = "tesser:stage";
const CHUNK_DELETE_BATCH = 1000;
const PARAMETER_DELETE_BATCH = 10;
const MAX_IDS = 100;
const MAX_LISTED = 1000;

const pattern = (re: RegExp) => Schema.String.check(Schema.isPattern(re));
const between = (minimum: number, maximum: number) => Schema.Int.check(Schema.isBetween({ minimum, maximum }));
const most = <S extends Schema.Top>(item: S, n: number) => Schema.Array(item).check(Schema.isMaxLength(n));
const some = <S extends Schema.Top>(item: S, n: number) => Schema.Array(item).check(Schema.isMinLength(1), Schema.isMaxLength(n));
const text = (n: number) => Schema.String.check(Schema.isMaxLength(n));
const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const OrgId = pattern(ORG_ID_PATTERN);
const AccountId = pattern(/^\d{12}$/);
const Region = pattern(AWS_REGION_RE);
const InstanceId = pattern(/^i-[0-9a-f]{8,17}$/);
const VolumeId = pattern(/^vol-[0-9a-f]{8,17}$/);
const SubnetId = pattern(/^subnet-[0-9a-f]{8,17}$/);
const SecurityGroupId = pattern(/^sg-[0-9a-f]{8,17}$/);
const VpcId = pattern(/^vpc-[0-9a-f]{8,17}$/);
const ImageId = pattern(/^ami-[0-9a-f]{8,17}$/);
export const InstanceType = pattern(/^[a-z][a-z0-9-]*\.[a-z0-9-]+$/);
const Zone = pattern(/^[a-z]{2}(-[a-z]+)+-\d{1,2}[a-z]$/);
const Device = pattern(/^\/dev\/sd[f-p]$/);
export const ChunkId = pattern(/^[0-9a-f]{64}$/);
const BoxId = pattern(BOX_ID_PATTERN);
const HostId = pattern(HOST_ID);
const ClientToken = pattern(/^[\w-]{1,64}$/);
const Bucket = pattern(/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/);
const ProfileName = pattern(/^[\w+=,.@-]{1,128}$/);
const ProfileArn = pattern(/^arn:aws:iam::\d{12}:instance-profile\/[\w+=,.@/-]+$/);
const RoleArn = pattern(/^arn:aws:iam::\d{12}:role\/[\w+=,.@/-]+$/);
const ZoneId = pattern(/^[a-z]{3,4}\d{1,2}-az\d{1,2}$/);
const Day = pattern(/^\d{4}-\d{2}-\d{2}$/);
const ObjectKey = Schema.String.check(Schema.isBetweenLength(1, 1024));
const ParameterName = pattern(/^\/[\w.\-/]{1,1010}$/);
const Sha256 = pattern(/^[0-9a-f]{64}$/);
const Version = pattern(/^\d+\.\d+\.\d+$/);
const CellUrl = Schema.String.check(Schema.isMaxLength(200), Schema.isPattern(/^https:\/\/[a-z0-9.-]+(:\d{1,5})?\/?$/));
const Tags = Schema.Record(text(128), text(256)).check(Schema.makeFilter((t: Record<string, string>) => Object.keys(t).length <= 50));

const ImageSource = Schema.Union([Schema.Struct({ id: ImageId }), Schema.Struct({ owner: AccountId })]);

export const ActuatorLimits = Schema.Struct({
  maxInstances: between(1, 10_000),
  mutationsPerMinute: between(1, 10_000),
  launchesPerHour: between(1, 10_000),
});
export type ActuatorLimits = typeof ActuatorLimits.Type;

const seat = {
  cellUrl: CellUrl,
  accountId: AccountId,
  region: Region,
  vpcId: VpcId,
  subnetIds: some(SubnetId, 8),
  securityGroupId: SecurityGroupId,
  box: Schema.Struct({ image: ImageSource, instanceProfile: ProfileName }),
  fleet: Schema.Struct({ image: ImageSource, instanceProfileArn: ProfileArn }),
  limits: ActuatorLimits,
};

export const SelfHostConfig = Schema.Struct({
  kind: Schema.Literal("selfHost"),
  ...seat,
  orgId: OrgId,
  tags: Tags.check(Schema.makeFilter((t: Record<string, string>) => cloudTagsProblem(t) ?? undefined)),
  snapshots: Schema.Struct({ bucket: Bucket }),
  parameterStore: Schema.Boolean,
});
export type SelfHostConfig = typeof SelfHostConfig.Type;

export const HostedConfig = Schema.Struct({
  kind: Schema.Literal("hosted"),
  ...seat,
  snapshots: Schema.Struct({ bucket: Bucket, orgRoleArn: RoleArn }),
  images: Schema.NullOr(Schema.Struct({ buckets: Schema.Record(Region, Bucket), readerRoleArn: RoleArn })),
  hosts: Schema.Struct({ bucket: Bucket }),
});
export type HostedConfig = typeof HostedConfig.Type;

export const ActuatorConfig = Schema.Union([SelfHostConfig, HostedConfig]);
export type ActuatorConfig = typeof ActuatorConfig.Type;
export type ActuatorKind = ActuatorConfig["kind"];

export const actuatorUrl = (config: ActuatorConfig) =>
  `${config.cellUrl.replace(/\/$/, "").replace(/^https:/, "wss:")}/v1/actuator/${config.kind === "selfHost" ? config.orgId : `hosted/${config.region}`}`;

export const ACTUATOR_PING = JSON.stringify({ to: "server", m: { _tag: "Ping" } });
export const ACTUATOR_PONG = JSON.stringify({ to: "client", m: { _tag: "Pong" } });

const Service = Schema.Literals(["ec2", "ssm", "s3", "servicequotas", "iam", "sts", "ce"]);

export class AwsRefused extends Schema.TaggedError<AwsRefused>()("AwsRefused", { code: Schema.String, message: Schema.String }) {}
export class AwsUnanswered extends Schema.TaggedError<AwsUnanswered>()("AwsUnanswered", { service: Service, action: text(64), message: text(1000) }) {}
const Fence = Schema.Literals(["instances", "not-configured", "org", "hosted-only", "self-host-only"]);
export class FenceRefused extends Schema.TaggedError<FenceRefused>()("FenceRefused", { fence: Fence, message: text(1000) }) {}
export class RateLimited extends Schema.TaggedError<RateLimited>()("RateLimited", { retryAfterMs: between(1, 3_600_000) }) {}
export class HelloRefused extends Schema.TaggedError<HelloRefused>()("HelloRefused", { message: text(1000) }) {}

export interface AwsErrorClass {
  readonly identifier: string;
  is(u: unknown): boolean;
  new (props: { readonly message?: string }): YieldableError & { readonly _tag: string };
}

const answered = <C extends AwsErrorClass>(C: C) =>
  Schema.Struct({ _tag: Schema.Literal(C.identifier), message: Schema.optionalKey(Schema.String) }).pipe(
    Schema.decodeTo(
      Schema.declare((u): u is InstanceType<C> => C.is(u)),
      SchemaTransformation.transform({
        decode: ({ message }) => new C(message === undefined ? {} : { message }) as InstanceType<C>,
        encode: (e) => ({ _tag: e._tag, message: e.message }),
      }),
    ),
  );

const refusedOnWire = Schema.Struct({ _tag: Schema.String, message: Schema.optionalKey(Schema.String) }).pipe(
  Schema.decodeTo(
    AwsRefused,
    SchemaTransformation.transform({
      decode: ({ _tag, message }) => ({ _tag: "AwsRefused" as const, code: _tag, message: message ?? "" }),
      encode: ({ code, message }) => ({ _tag: code, message }),
    }),
  ),
);

const refusals = <const K extends readonly AwsErrorClass[]>(known: K) => Schema.Union([...known.map(answered), AwsUnanswered, FenceRefused, RateLimited, refusedOnWire]);

export type AwsAnswer = UnknownAwsError | { readonly _tag: string; readonly message?: string | undefined };

export const refusal =
  <const K extends readonly AwsErrorClass[]>(known: K) =>
  (e: AwsAnswer): InstanceType<K[number]> | AwsRefused => {
    const code = UnknownAwsError.is(e) ? (e.code ?? String(e.status)) : e._tag;
    const message = e.message ?? "";
    const C = known.find((k) => k.identifier === code);
    return C === undefined ? new AwsRefused({ code, message }) : (new C({ message }) as InstanceType<K[number]>);
  };

export const KnownAwsErrors = {
  RunBox: [InsufficientInstanceCapacity, RequestLimitExceeded],
  RunHost: [InsufficientInstanceCapacity, Unsupported, RequestLimitExceeded],
  StopInstance: [IncorrectInstanceState, InvalidInstanceIDNotFound],
  StartInstance: [InsufficientInstanceCapacity],
  TagInstances: [InvalidInstanceIDNotFound],
  DryRun: [UnauthorizedOperation],
  DescribeInstances: [InvalidInstanceIDNotFound],
  DescribeRouteTables: [UnauthorizedOperation],
  ProbeVersionDelete: [AccessDenied, AccessDeniedException],
  None: [],
} as const;

const Refused = refusals(KnownAwsErrors.None);

const InstanceState = Schema.Literals(["pending", "running", "stopping", "stopped", "shutting-down", "terminated"]);
const StopHow = Schema.Literals(["stop", "hibernate", "hibernate-only", "force", "halt"]);

const runBox = {
  clientToken: ClientToken,
  orgId: OrgId,
  boxId: BoxId,
  instanceType: InstanceType,
  subnetId: SubnetId,
  securityGroupId: SecurityGroupId,
  imageId: ImageId,
  rootGb: between(8, 16_384),
  memoryGb: between(1, 1024),
  homeGb: Schema.NullOr(between(1, 16_384)),
  throughputMbps: Schema.NullOr(between(125, 2000)),
  tags: Tags,
};
const runHost = {
  clientToken: ClientToken,
  orgId: Schema.NullOr(OrgId),
  hostId: HostId,
  instanceType: InstanceType,
  subnetId: SubnetId,
  imageId: ImageId,
  release: Schema.Struct({ hostd: HostdBuild, image: Schema.Struct({ id: pattern(/^[a-z0-9-]{1,64}$/), kernel: Sha256, root: Sha256 }) }),
  disk: Schema.Struct({ gb: between(1, 16_384), iops: between(3000, 80_000), mbps: between(125, 2000) }),
  tags: Tags,
};
const stopInstance = { instanceId: InstanceId, how: StopHow };
const startInstance = { instanceId: InstanceId };
const rebootInstances = { instanceIds: some(InstanceId, MAX_IDS) };
const terminateInstances = { instanceIds: some(InstanceId, MAX_IDS), skipOsShutdown: Schema.optionalKey(Schema.Boolean) };
const tagInstances = { instanceIds: some(InstanceId, MAX_IDS), tags: Tags };
const createVolume = {
  clientToken: ClientToken,
  orgId: OrgId,
  zone: Zone,
  sizeGb: between(1, 16_384),
  iops: Schema.NullOr(between(3000, 80_000)),
  throughputMbps: Schema.NullOr(between(125, 2000)),
  tags: Tags,
};
const attachVolume = { volumeId: VolumeId, instanceId: InstanceId, device: Device };
const detachVolume = { volumeId: VolumeId };
const deleteVolume = { volumeId: VolumeId };
const modifyVolume = { volumeId: VolumeId, throughputMbps: between(125, 2000), iops: between(3000, 80_000) };

const Ec2Mutation = Schema.Union([
  Schema.TaggedStruct("RunBox", runBox),
  Schema.TaggedStruct("RunHost", runHost),
  Schema.TaggedStruct("StopInstance", stopInstance),
  Schema.TaggedStruct("StartInstance", startInstance),
  Schema.TaggedStruct("RebootInstances", rebootInstances),
  Schema.TaggedStruct("TerminateInstances", terminateInstances),
  Schema.TaggedStruct("TagInstances", tagInstances),
  Schema.TaggedStruct("CreateVolume", createVolume),
  Schema.TaggedStruct("AttachVolume", attachVolume),
  Schema.TaggedStruct("DetachVolume", detachVolume),
  Schema.TaggedStruct("DeleteVolume", deleteVolume),
  Schema.TaggedStruct("ModifyVolume", modifyVolume),
]);

const Instance = Schema.Struct({
  instanceId: InstanceId,
  state: InstanceState,
  instanceType: Schema.NullOr(InstanceType),
  zone: Schema.NullOr(Zone),
  subnetId: Schema.NullOr(SubnetId),
  privateIp: Schema.NullOr(text(15)),
  publicIp: Schema.NullOr(text(15)),
  launchedAt: Schema.NullOr(Count),
  profileArn: Schema.NullOr(ProfileArn),
  tags: Tags,
  reason: Schema.optionalKey(Schema.NullOr(text(512))),
});

export const InstanceQuery = Schema.Struct({
  orgId: Schema.NullOr(OrgId),
  instanceIds: Schema.optionalKey(some(InstanceId, MAX_IDS)),
  tag: Schema.optionalKey(Schema.Struct({ key: Schema.Literals(["tesser:boxId", FLEET_HOST_TAG]), value: Schema.optionalKey(text(256)) })),
  stage: Schema.optionalKey(text(256)),
});
export type InstanceQuery = typeof InstanceQuery.Type;

const Volume = Schema.Struct({
  volumeId: VolumeId,
  state: Schema.Literals(["creating", "available", "in-use", "deleting", "deleted", "error"]),
  attachedTo: Schema.NullOr(InstanceId),
  attachState: Schema.NullOr(Schema.Literals(["attaching", "attached", "detaching", "detached", "busy"])),
  device: Schema.NullOr(text(32)),
  sizeGb: Count,
  throughputMbps: Schema.NullOr(Count),
  tags: Tags,
});

export const VolumeQuery = Schema.Struct({
  orgId: OrgId,
  volumeIds: Schema.optionalKey(some(VolumeId, MAX_IDS)),
  attachedTo: Schema.optionalKey(InstanceId),
  device: Schema.optionalKey(Schema.Literals(["/dev/sda1", "/dev/sdf"])),
  moving: Schema.optionalKey(Schema.Literal(true)),
});
export type VolumeQuery = typeof VolumeQuery.Type;

const VolumeModification = Schema.Struct({ state: Schema.Literals(["modifying", "optimizing", "completed", "failed"]), startMs: Count, targetMbps: Schema.NullOr(Count) });

export const VcpuUsage = Schema.Struct({ onDemand: Count, standardOnDemand: Count, orgHosts: Count });
export type VcpuUsage = typeof VcpuUsage.Type;

const Subnet = Schema.Struct({ subnetId: SubnetId, vpcId: VpcId, zone: Schema.NullOr(Zone), zoneId: Schema.NullOr(text(32)), cidrBlock: Schema.NullOr(text(18)), mapPublicIpOnLaunch: Schema.Boolean });
const Permission = Schema.Struct({
  protocol: text(8),
  fromPort: Schema.NullOr(between(-1, 65535)),
  toPort: Schema.NullOr(between(-1, 65535)),
  cidrs: most(text(64), 256),
  groupIds: most(SecurityGroupId, 256),
  prefixLists: most(text(32), 256),
});
const SecurityGroup = Schema.Struct({ groupId: SecurityGroupId, vpcId: VpcId, ingress: most(Permission, 256) });
export const AwsCredentials = Schema.Struct({
  accessKeyId: pattern(/^[A-Z0-9]{16,128}$/),
  secretAccessKey: Schema.Redacted(text(128)),
  sessionToken: Schema.Redacted(text(8192)),
  expiration: Count,
});
export type AwsCredentials = typeof AwsCredentials.Type;

const CostDay = Schema.Struct({ day: Day, estimated: Schema.Boolean, services: most(Schema.Struct({ service: text(256), amount: Schema.Finite }), 500) });
const RouteTable = Schema.Struct({ routeTableId: text(32), main: Schema.Boolean, subnetIds: most(SubnetId, 256), routes: most(Schema.Struct({ destination: text(64), target: text(64) }), 256) });

export class ActuatorRpcs extends RpcGroup.make(
  Rpc.make("RunBox", {
    payload: runBox,
    success: Schema.Struct({ instanceId: InstanceId, privateIp: Schema.NullOr(text(15)), publicIp: Schema.NullOr(text(15)), hibernation: Schema.Boolean }),
    error: refusals(KnownAwsErrors.RunBox),
  }),
  Rpc.make("RunHost", { payload: runHost, success: Schema.Struct({ instanceId: InstanceId }), error: refusals(KnownAwsErrors.RunHost) }),
  Rpc.make("StopInstance", { payload: stopInstance, success: Schema.Struct({ hibernating: Schema.Boolean, refused: Schema.NullOr(text(128)) }), error: refusals(KnownAwsErrors.StopInstance) }),
  Rpc.make("StartInstance", { payload: startInstance, success: Schema.Literals(["started", "not-yet"]), error: refusals(KnownAwsErrors.StartInstance) }),
  Rpc.make("RebootInstances", { payload: rebootInstances, error: Refused }),
  Rpc.make("TerminateInstances", { payload: terminateInstances, error: Refused }),
  Rpc.make("TagInstances", { payload: tagInstances, error: refusals(KnownAwsErrors.TagInstances) }),
  Rpc.make("CreateVolume", { payload: createVolume, success: Schema.Struct({ volumeId: VolumeId }), error: Refused }),
  Rpc.make("AttachVolume", { payload: attachVolume, error: Refused }),
  Rpc.make("DetachVolume", { payload: detachVolume, error: Refused }),
  Rpc.make("DeleteVolume", { payload: deleteVolume, success: Schema.Literals(["deleted", "in-use", "gone"]), error: Refused }),
  Rpc.make("ModifyVolume", { payload: modifyVolume, error: Refused }),
  Rpc.make("DryRun", { payload: { mutation: Ec2Mutation }, error: refusals(KnownAwsErrors.DryRun) }),
  Rpc.make("DescribeInstances", { payload: InstanceQuery, success: most(Instance, MAX_LISTED), error: refusals(KnownAwsErrors.DescribeInstances) }),
  Rpc.make("DescribeVolumes", { payload: VolumeQuery, success: most(Volume, MAX_LISTED), error: Refused }),
  Rpc.make("VolumeModifications", { payload: { volumeId: VolumeId }, success: most(VolumeModification, 100), error: Refused }),
  Rpc.make("VcpuUsage", { payload: { orgId: Schema.NullOr(OrgId) }, success: VcpuUsage, error: Refused }),
  Rpc.make("VcpuQuota", { success: Schema.Struct({ vcpus: Count }), error: Refused }),
  Rpc.make("DescribeSubnets", { success: most(Subnet, 8), error: Refused }),
  Rpc.make("DescribeSecurityGroup", { success: Schema.NullOr(SecurityGroup), error: Refused }),
  Rpc.make("DescribeRouteTables", { success: most(RouteTable, 200), error: refusals(KnownAwsErrors.DescribeRouteTables) }),
  Rpc.make("FederationInfo", { success: Schema.Struct({ enabled: Schema.Boolean }), error: Refused }),
  Rpc.make("PutParameter", { payload: { name: ParameterName, value: Schema.Redacted(text(8192)) }, error: Refused }),
  Rpc.make("DeleteParameters", { payload: { names: some(ParameterName, PARAMETER_DELETE_BATCH) }, error: Refused }),
  Rpc.make("DeleteChunks", {
    payload: { ids: some(ChunkId, CHUNK_DELETE_BATCH) },
    success: Schema.Struct({ removed: most(ChunkId, CHUNK_DELETE_BATCH), refused: most(Schema.Struct({ id: ChunkId, message: text(300) }), CHUNK_DELETE_BATCH) }),
    error: Refused,
  }),
  Rpc.make("ListChunks", { success: Schema.Struct({ any: Schema.Boolean }), error: Refused }),
  Rpc.make("BucketVersioning", { success: Schema.Struct({ status: Schema.Literals(["Enabled", "Suspended", ""]) }), error: Refused }),
  Rpc.make("BucketLifecycle", { success: Schema.Struct({ expiresNoncurrent: Schema.Boolean }), error: Refused }),
  Rpc.make("ProbeVersionDelete", { error: refusals(KnownAwsErrors.ProbeVersionDelete) }),
  Rpc.make("EnsureOrgSecurityGroup", { payload: { orgId: OrgId }, success: Schema.Struct({ groupId: SecurityGroupId }), error: Refused }),
  Rpc.make("DeleteSecurityGroup", { payload: { groupId: SecurityGroupId }, success: Schema.Literals(["deleted", "in-use"]), error: Refused }),
  Rpc.make("CostAndUsage", { payload: { start: Day, end: Day }, success: most(CostDay, 400), error: Refused }),
  Rpc.make("InstanceTypeOfferings", { payload: { region: Region, zoneId: ZoneId, instanceTypes: some(InstanceType, 100) }, success: most(InstanceType, 100), error: Refused }),
  Rpc.make("ImagesCredentials", {
    payload: { region: Region, hostId: Schema.Union([HostId, Schema.Literal("landing")]), keys: some(ObjectKey, MAX_FLEET_FILES) },
    success: AwsCredentials,
    error: Refused,
  }),
  Rpc.make("OrgS3Credentials", { payload: { orgId: OrgId }, success: AwsCredentials, error: Refused }),
) {}

export type ActuatorOp = RpcGroup.Rpcs<typeof ActuatorRpcs>["_tag"];

export type Audit = "mutation" | "grant" | "read";
export type Serves = "both" | "hosted" | "selfHost";
type Retry = "safe" | "unsent-only";

export const OPS: { readonly [K in ActuatorOp]: { readonly audit: Audit; readonly serves: Serves; readonly retry: Retry } } = {
  RunBox: { audit: "mutation", serves: "both", retry: "safe" },
  RunHost: { audit: "mutation", serves: "both", retry: "safe" },
  StopInstance: { audit: "mutation", serves: "both", retry: "unsent-only" },
  StartInstance: { audit: "mutation", serves: "both", retry: "safe" },
  RebootInstances: { audit: "mutation", serves: "both", retry: "unsent-only" },
  TerminateInstances: { audit: "mutation", serves: "both", retry: "safe" },
  TagInstances: { audit: "mutation", serves: "both", retry: "safe" },
  CreateVolume: { audit: "mutation", serves: "both", retry: "safe" },
  AttachVolume: { audit: "mutation", serves: "both", retry: "unsent-only" },
  DetachVolume: { audit: "mutation", serves: "both", retry: "unsent-only" },
  DeleteVolume: { audit: "mutation", serves: "both", retry: "safe" },
  ModifyVolume: { audit: "mutation", serves: "both", retry: "unsent-only" },
  DryRun: { audit: "read", serves: "both", retry: "safe" },
  DescribeInstances: { audit: "read", serves: "both", retry: "safe" },
  DescribeVolumes: { audit: "read", serves: "both", retry: "safe" },
  VolumeModifications: { audit: "read", serves: "both", retry: "safe" },
  VcpuUsage: { audit: "read", serves: "both", retry: "safe" },
  VcpuQuota: { audit: "read", serves: "both", retry: "safe" },
  DescribeSubnets: { audit: "read", serves: "both", retry: "safe" },
  DescribeSecurityGroup: { audit: "read", serves: "both", retry: "safe" },
  DescribeRouteTables: { audit: "read", serves: "both", retry: "safe" },
  FederationInfo: { audit: "read", serves: "selfHost", retry: "safe" },
  PutParameter: { audit: "mutation", serves: "selfHost", retry: "safe" },
  DeleteParameters: { audit: "mutation", serves: "selfHost", retry: "safe" },
  DeleteChunks: { audit: "mutation", serves: "selfHost", retry: "safe" },
  ListChunks: { audit: "read", serves: "selfHost", retry: "safe" },
  BucketVersioning: { audit: "read", serves: "selfHost", retry: "safe" },
  BucketLifecycle: { audit: "read", serves: "selfHost", retry: "safe" },
  ProbeVersionDelete: { audit: "mutation", serves: "selfHost", retry: "safe" },
  EnsureOrgSecurityGroup: { audit: "mutation", serves: "hosted", retry: "safe" },
  DeleteSecurityGroup: { audit: "mutation", serves: "hosted", retry: "safe" },
  CostAndUsage: { audit: "read", serves: "hosted", retry: "safe" },
  InstanceTypeOfferings: { audit: "read", serves: "hosted", retry: "safe" },
  ImagesCredentials: { audit: "grant", serves: "hosted", retry: "safe" },
  OrgS3Credentials: { audit: "grant", serves: "hosted", retry: "safe" },
};

const Refusal = Schema.Struct({ _tag: text(128), message: Schema.optionalKey(Schema.String) });
export const AuditLine = Schema.Union([
  Schema.TaggedStruct("Mutation", {
    at: Count,
    op: text(64),
    orgId: Schema.NullOr(OrgId),
    request: Schema.String,
    outcome: Schema.Union([Schema.Struct({ ok: Schema.String }), Schema.Struct({ refused: Refusal })]),
  }),
  Schema.TaggedStruct("Grant", {
    at: Count,
    op: text(64),
    orgId: Schema.NullOr(OrgId),
    request: Schema.String,
    outcome: Schema.Union([Schema.Struct({ accessKeyId: Schema.String, expiration: Count }), Schema.Struct({ refused: Refusal })]),
  }),
  Schema.TaggedStruct("Refused", { at: Count, op: text(64), orgId: Schema.NullOr(OrgId), request: Schema.String, refused: Refusal }),
  Schema.TaggedStruct("Reads", { minute: Count, op: text(64), calls: Count, refused: Count }),
]);
export type AuditLine = typeof AuditLine.Type;

const Hello = Schema.Struct({
  version: Version,
  ops: most(text(64), 256),
  config: ActuatorConfig,
  identity: Schema.String.check(Schema.isBetweenLength(1, 8192)),
});
export type ActuatorHello = typeof Hello.Type;

const Health = Schema.Struct({ startedAt: Count, calls: Count, refused: Count, limited: Count, lastError: Schema.NullOr(text(1000)) });

export class ActuatorCellRpcs extends RpcGroup.make(Rpc.make("Hello", { payload: Hello, error: HelloRefused }), Rpc.make("Health", { payload: Health }), Rpc.make("Draining")) {}
