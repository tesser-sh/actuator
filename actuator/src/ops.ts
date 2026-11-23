import * as ce from "@distilled.cloud/aws/cost-explorer";
import type { Credentials } from "@distilled.cloud/aws/Credentials";
import * as ec2 from "@distilled.cloud/aws/ec2";
import * as iam from "@distilled.cloud/aws/iam";
import * as s3 from "@distilled.cloud/aws/s3";
import * as quotas from "@distilled.cloud/aws/service-quotas";
import * as ssm from "@distilled.cloud/aws/ssm";
import * as sts from "@distilled.cloud/aws/sts";
import { VCPU_QUOTA_CODE } from "@tesser/protocol/aws";
import {
  boxUserData,
  chunkKeys,
  hostUserData,
  imagesSession,
  instanceFilters,
  launchAdmitted,
  launchTags,
  orgGroup,
  orgS3Session,
  orgTags,
  stateReason,
  vcpuUsage,
  volumeFilters,
  type Filter,
} from "@tesser/protocol/actuator-fence";
import { ActuatorRpcs, AwsUnanswered, FenceRefused, KnownAwsErrors, type ActuatorConfig, type AwsCredentials, type HostedConfig, type SelfHostConfig } from "@tesser/protocol/actuator-rpc";
import { chunkKey, chunkPrefix } from "@tesser/protocol/store";
import { Clock, Effect, Redacted, Semaphore } from "effect";
import type { HttpClient } from "effect/http";
import type { Rpc, RpcGroup } from "effect/rpc";
import { aws, awsCode, onCode, plain, tagList, tagsOf } from "./aws.ts";
import type { Guard } from "./guard.ts";

type Payload<T extends RpcGroup.Rpcs<typeof ActuatorRpcs>["_tag"]> = Rpc.Payload<Extract<RpcGroup.Rpcs<typeof ActuatorRpcs>, { readonly _tag: T }>>;
type Mutation = Payload<"DryRun">["mutation"];
type AwsEnv = Credentials | HttpClient.HttpClient;

const MAX_LISTED = 1000;
const RECENT_MS = 5 * 60_000;
const LIVE = ["pending", "running", "stopping", "stopped"];
const BURNING = ["pending", "running", "shutting-down", "stopping"];
const STATES = new Set(["pending", "running", "stopping", "stopped", "shutting-down", "terminated"] as const);
const VOLUME_STATES = new Set(["creating", "available", "in-use", "deleting", "deleted", "error"] as const);
const ATTACH_STATES = new Set(["attaching", "attached", "detaching", "detached", "busy"] as const);
const MODIFICATION_STATES = new Set(["modifying", "optimizing", "completed", "failed"] as const);
const HIBERNATE_REFUSED = ["UnsupportedHibernationConfiguration", "UnsupportedOperation"];
const HIBERNATE_NOT_READY = "not_ready";
const PROBE_CHUNK = "0".repeat(64);

const member = <T extends string>(set: ReadonlySet<T>, v: string | undefined): v is T => v !== undefined && (set as ReadonlySet<string>).has(v);
const filters = (list: readonly Filter[]) => list.map((f) => ({ Name: f.Name, Values: [...f.Values] }));
const notReady = (e: unknown) => awsCode(e) === "UnsupportedOperation" && /not ready to hibernate/i.test((e as { message?: string }).message ?? "");

const gp3 = (sizeGb: number, iops: number | null, throughputMbps: number | null) => {
  const capped = iops === null ? null : Math.min(iops, 16_000, 500 * sizeGb);
  const io = capped === null || capped <= 3000 ? null : capped;
  const mbps = throughputMbps === null ? null : Math.max(125, Math.min(throughputMbps, 1000, Math.floor((io ?? 3000) / 4)));
  return { ...(io === null ? {} : { Iops: io }), ...(mbps === null ? {} : { Throughput: mbps }) };
};

const instanceOf = (i: ec2.Instance) =>
  i.InstanceId === undefined || !member(STATES, i.State?.Name)
    ? []
    : [
        {
          instanceId: i.InstanceId,
          state: i.State.Name,
          instanceType: i.InstanceType ?? null,
          zone: i.Placement?.AvailabilityZone ?? null,
          subnetId: i.SubnetId ?? null,
          privateIp: i.PrivateIpAddress ?? null,
          publicIp: i.PublicIpAddress ?? null,
          launchedAt: i.LaunchTime === undefined ? null : new Date(i.LaunchTime).getTime(),
          profileArn: i.IamInstanceProfile?.Arn ?? null,
          tags: tagsOf(i.Tags),
          reason: stateReason(i.StateReason),
        },
      ];

const volumeOf = (v: ec2.Volume) => {
  const attachment = v.Attachments?.[0];
  if (v.VolumeId === undefined || !member(VOLUME_STATES, v.State)) return [];
  return [
    {
      volumeId: v.VolumeId,
      state: v.State,
      attachedTo: attachment?.InstanceId ?? null,
      attachState: member(ATTACH_STATES, attachment?.State) ? attachment.State : null,
      device: attachment?.Device ?? null,
      sizeGb: v.Size ?? 0,
      throughputMbps: v.Throughput ?? null,
      tags: tagsOf(v.Tags),
    },
  ];
};

const granted = (action: string, c: sts.Credentials | undefined) =>
  c === undefined
    ? Effect.fail(new AwsUnanswered({ service: "sts", action, message: "AssumeRole answered without credentials" }))
    : Effect.succeed<AwsCredentials>({
        accessKeyId: c.AccessKeyId,
        secretAccessKey: Redacted.make(plain(c.SecretAccessKey)),
        sessionToken: Redacted.make(plain(c.SessionToken)),
        expiration: new Date(c.Expiration).getTime(),
      });

const describeInstances = Effect.fn("Ops.describeInstances")(function* (request: ec2.DescribeInstancesRequest, limit: number) {
  const out: ec2.Instance[] = [];
  let token: string | undefined;
  do {
    const page = yield* ec2.describeInstances({ ...request, ...(token === undefined ? {} : { NextToken: token }) });
    out.push(...(page.Reservations ?? []).flatMap((r) => r.Instances ?? []));
    token = page.NextToken === "" ? undefined : page.NextToken;
  } while (token !== undefined && out.length < limit);
  return out.slice(0, limit);
});

const boxLaunch = (config: ActuatorConfig, org: string, p: Payload<"RunBox">, hibernate: boolean): ec2.RunInstancesRequest => {
  const ebs = (gb: number) => ({ VolumeSize: gb, VolumeType: "gp3" as const, ...(p.throughputMbps === null ? {} : { Throughput: p.throughputMbps }), DeleteOnTermination: true, Encrypted: true });
  const tags = tagList(launchTags(config, org, p.tags));
  return {
    ImageId: p.imageId,
    InstanceType: p.instanceType as ec2.InstanceType,
    MinCount: 1,
    MaxCount: 1,
    UserData: btoa(boxUserData(config, org, p.boxId)),
    ClientToken: p.clientToken,
    SubnetId: p.subnetId,
    SecurityGroupIds: [p.securityGroupId],
    IamInstanceProfile: { Name: config.box.instanceProfile },
    BlockDeviceMappings: [{ DeviceName: "/dev/sda1", Ebs: ebs(p.rootGb + (hibernate ? p.memoryGb : 0)) }, ...(p.homeGb === null ? [] : [{ DeviceName: "/dev/sdf", Ebs: ebs(p.homeGb) }])],
    ...(hibernate ? { HibernationOptions: { Configured: true } } : {}),
    MetadataOptions: { HttpEndpoint: "enabled", HttpTokens: "required", HttpPutResponseHopLimit: 1 },
    TagSpecifications: [
      { ResourceType: "instance", Tags: tags },
      { ResourceType: "volume", Tags: tags },
    ],
  };
};

const hostLaunch = (config: ActuatorConfig, org: string | null, p: Payload<"RunHost">): ec2.RunInstancesRequest => {
  const tags = tagList(launchTags(config, org, p.tags));
  return {
    ImageId: p.imageId,
    InstanceType: p.instanceType as ec2.InstanceType,
    MinCount: 1,
    MaxCount: 1,
    ClientToken: p.clientToken,
    UserData: btoa(hostUserData(config, org, p.hostId, p.release)),
    SubnetId: p.subnetId,
    SecurityGroupIds: [config.securityGroupId],
    IamInstanceProfile: { Arn: config.fleet.instanceProfileArn },
    BlockDeviceMappings: [{ DeviceName: "/dev/sdf", Ebs: { VolumeSize: p.disk.gb, VolumeType: "gp3", Iops: p.disk.iops, Throughput: p.disk.mbps, DeleteOnTermination: true, Encrypted: true } }],
    MetadataOptions: { HttpEndpoint: "enabled", HttpTokens: "required", HttpPutResponseHopLimit: 1 },
    TagSpecifications: [
      { ResourceType: "instance", Tags: tags },
      { ResourceType: "volume", Tags: tags },
    ],
  };
};

const volumeRequest = (org: string, p: Payload<"CreateVolume">): ec2.CreateVolumeRequest => ({
  ClientToken: p.clientToken,
  AvailabilityZone: p.zone,
  Size: p.sizeGb,
  VolumeType: "gp3",
  Encrypted: true,
  ...gp3(p.sizeGb, p.iops, p.throughputMbps),
  TagSpecifications: [{ ResourceType: "volume", Tags: tagList(orgTags(org, p.tags)) }],
});

const dryCallFor =
  (config: ActuatorConfig, home: string | null) =>
  (m: Exclude<Mutation, { readonly _tag: "RunHost" }>): Effect.Effect<unknown, unknown, AwsEnv> => {
    switch (m._tag) {
      case "RunBox":
        return ec2.runInstances({ ...boxLaunch(config, m.orgId, m, true), DryRun: true });
      case "StopInstance":
        return ec2.stopInstances({ InstanceIds: [m.instanceId], DryRun: true });
      case "StartInstance":
        return ec2.startInstances({ InstanceIds: [m.instanceId], DryRun: true });
      case "RebootInstances":
        return ec2.rebootInstances({ InstanceIds: [...m.instanceIds], DryRun: true });
      case "TerminateInstances":
        return ec2.terminateInstances({ InstanceIds: [...m.instanceIds], DryRun: true });
      case "TagInstances":
        return ec2.createTags({ Resources: [...m.instanceIds], Tags: tagList(orgTags(home, m.tags)), DryRun: true });
      case "CreateVolume":
        return ec2.createVolume({ ...volumeRequest(m.orgId, m), DryRun: true });
      case "AttachVolume":
        return ec2.attachVolume({ VolumeId: m.volumeId, InstanceId: m.instanceId, Device: m.device, DryRun: true });
      case "DetachVolume":
        return ec2.detachVolume({ VolumeId: m.volumeId, DryRun: true });
      case "DeleteVolume":
        return ec2.deleteVolume({ VolumeId: m.volumeId, DryRun: true });
      case "ModifyVolume":
        return ec2.modifyVolume({ VolumeId: m.volumeId, Throughput: m.throughputMbps, Iops: m.iops, DryRun: true });
    }
  };

export const makeOps = (config: ActuatorConfig, guard: Guard) =>
  ActuatorRpcs.toLayer(
    Effect.gen(function* () {
      const launching = yield* Semaphore.make(1);
      const writing = yield* Semaphore.make(1);
      const recent = new Map<string, { org: string | null; at: number }>();
      const home = config.kind === "selfHost" ? config.orgId : null;
      const dryCall = dryCallFor(config, home);

      const selfHost: Effect.Effect<SelfHostConfig, FenceRefused> =
        config.kind === "selfHost" ? Effect.succeed(config) : Effect.fail(new FenceRefused({ fence: "self-host-only", message: "this runs only on a self-hosted org's actuator" }));
      const hosted: Effect.Effect<HostedConfig, FenceRefused> =
        config.kind === "hosted" ? Effect.succeed(config) : Effect.fail(new FenceRefused({ fence: "hosted-only", message: "this runs only on tesser's own hosted actuators" }));
      const bucket = Effect.map(selfHost, (c) => ({ config: c, bucket: c.snapshots.bucket }));
      const parameters = Effect.flatMap(selfHost, (c) =>
        c.parameterStore ? Effect.void : Effect.fail(new FenceRefused({ fence: "not-configured", message: "this actuator does not write Parameter Store" })),
      );

      const live = Effect.fn("Ops.live")(function* (org: string | null) {
        const now = yield* Clock.currentTimeMillis;
        for (const [id, r] of recent) if (r.at < now - RECENT_MS) recent.delete(id);
        const found = yield* describeInstances(
          { Filters: [...filters(instanceFilters(org, { orgId: org })), { Name: "instance-state-name", Values: LIVE }], MaxResults: MAX_LISTED },
          Number.POSITIVE_INFINITY,
        ).pipe(aws("ec2", "DescribeInstances", KnownAwsErrors.None));
        const ids = new Set(found.flatMap((i) => i.InstanceId ?? []));
        for (const [id, r] of recent) if (r.org === org) ids.add(id);
        return ids.size;
      });

      const launch = <A extends { readonly instanceId: string }, E, R>(org: string | null, body: Effect.Effect<A, E, R>) =>
        Semaphore.withPermits(
          launching,
          1,
        )(
          Effect.gen(function* () {
            yield* Effect.fromResult(launchAdmitted(config.limits, yield* live(org)));
            const out = yield* body;
            recent.set(out.instanceId, { org, at: yield* Clock.currentTimeMillis });
            return out;
          }),
        );

      const firstInstance = (action: string, out: ec2.Reservation) => {
        const i = out.Instances?.[0];
        return i?.InstanceId === undefined
          ? Effect.fail(new AwsUnanswered({ service: "ec2", action, message: "RunInstances answered without an instance" }))
          : Effect.succeed({ ...i, InstanceId: i.InstanceId });
      };

      const dryRun = Effect.fn("Ops.dryRun")(function* (m: Mutation, org: string | null) {
        if (m._tag === "RunBox" || m._tag === "RunHost") yield* Effect.fromResult(launchAdmitted(config.limits, yield* live(org)));
        const call = m._tag === "RunHost" ? ec2.runInstances({ ...hostLaunch(config, org, m), DryRun: true }) : dryCall(m);
        return yield* call.pipe(
          Effect.asVoid,
          onCode(["DryRunOperation"], () => Effect.void),
          aws("ec2", m._tag, KnownAwsErrors.DryRun),
        );
      });

      return ActuatorRpcs.of({
        RunBox: (p) =>
          guard.run("RunBox", p, p.orgId, (org) =>
            launch(
              org,
              Effect.gen(function* () {
                const run = (hibernate: boolean) => ec2.runInstances(boxLaunch(config, org, p, hibernate)).pipe(Effect.map((out) => ({ out, hibernation: hibernate })));
                const { out, hibernation } = yield* run(true).pipe(
                  onCode(["UnsupportedHibernationConfiguration"], () => run(false)),
                  aws("ec2", "RunInstances", KnownAwsErrors.RunBox),
                );
                const i = yield* firstInstance("RunInstances", out);
                return { instanceId: i.InstanceId, privateIp: i.PrivateIpAddress ?? null, publicIp: i.PublicIpAddress ?? null, hibernation };
              }),
            ),
          ),

        RunHost: (p) =>
          guard.run("RunHost", p, p.orgId, (org) =>
            launch(
              org,
              Effect.gen(function* () {
                const out = yield* ec2.runInstances(hostLaunch(config, org, p)).pipe(aws("ec2", "RunInstances", KnownAwsErrors.RunHost));
                const i = yield* firstInstance("RunInstances", out);
                return { instanceId: i.InstanceId };
              }),
            ),
          ),

        StopInstance: (p) =>
          guard.run("StopInstance", p, undefined, () => {
            const stop = (extra: Partial<ec2.StopInstancesRequest>) => ec2.stopInstances({ InstanceIds: [p.instanceId], ...extra });
            const plainStop = stop(p.how === "force" ? { Force: true } : p.how === "halt" ? { SkipOsShutdown: true } : {});
            const stopped =
              p.how !== "hibernate" && p.how !== "hibernate-only"
                ? Effect.as(plainStop, { hibernating: false, refused: null as string | null })
                : stop({ Hibernate: true }).pipe(
                    Effect.as({ hibernating: true, refused: null as string | null }),
                    onCode(HIBERNATE_REFUSED, (e) =>
                      p.how === "hibernate-only"
                        ? Effect.succeed({ hibernating: false, refused: notReady(e) ? HIBERNATE_NOT_READY : awsCode(e) })
                        : Effect.as(plainStop, { hibernating: false, refused: awsCode(e) }),
                    ),
                  );
            return aws("ec2", "StopInstances", KnownAwsErrors.StopInstance)(stopped);
          }),

        StartInstance: (p) =>
          guard.run("StartInstance", p, undefined, () =>
            ec2.startInstances({ InstanceIds: [p.instanceId] }).pipe(
              Effect.as("started" as const),
              onCode(["IncorrectInstanceState"], (e) =>
                ec2.describeInstances({ InstanceIds: [p.instanceId] }).pipe(
                  Effect.map((out) => (out.Reservations ?? []).flatMap((r) => r.Instances ?? [])[0]?.State?.Name),
                  Effect.orElseSucceed(() => undefined),
                  Effect.flatMap((state) => (state === "shutting-down" || state === "terminated" ? Effect.fail(e) : Effect.succeed("not-yet" as const))),
                ),
              ),
              aws("ec2", "StartInstances", KnownAwsErrors.StartInstance),
            ),
          ),

        RebootInstances: (p) =>
          guard.run("RebootInstances", p, undefined, () => ec2.rebootInstances({ InstanceIds: [...p.instanceIds] }).pipe(Effect.asVoid, aws("ec2", "RebootInstances", KnownAwsErrors.None))),

        TerminateInstances: (p) =>
          guard.run("TerminateInstances", p, undefined, () => {
            const terminate = (ids: string[]) => ec2.terminateInstances({ InstanceIds: ids, ...(p.skipOsShutdown === true ? { SkipOsShutdown: true } : {}) }).pipe(Effect.asVoid);
            const gone = onCode(["InvalidInstanceID.NotFound"], () => Effect.void);
            return terminate([...p.instanceIds]).pipe(
              onCode(["InvalidInstanceID.NotFound"], () => (p.instanceIds.length <= 1 ? Effect.void : Effect.forEach(p.instanceIds, (id) => gone(terminate([id])), { discard: true }))),
              aws("ec2", "TerminateInstances", KnownAwsErrors.None),
            );
          }),

        TagInstances: (p) =>
          guard.run("TagInstances", p, undefined, (org) =>
            ec2.createTags({ Resources: [...p.instanceIds], Tags: tagList(orgTags(org, p.tags)) }).pipe(Effect.asVoid, aws("ec2", "CreateTags", KnownAwsErrors.TagInstances)),
          ),

        CreateVolume: (p) =>
          guard.run("CreateVolume", p, p.orgId, (org) =>
            ec2.createVolume(volumeRequest(org, p)).pipe(
              aws("ec2", "CreateVolume", KnownAwsErrors.None),
              Effect.flatMap((out) =>
                out.VolumeId === undefined
                  ? Effect.fail(new AwsUnanswered({ service: "ec2", action: "CreateVolume", message: "CreateVolume answered without a volume id" }))
                  : Effect.succeed({ volumeId: out.VolumeId }),
              ),
            ),
          ),

        AttachVolume: (p) =>
          guard.run("AttachVolume", p, undefined, () =>
            ec2.attachVolume({ VolumeId: p.volumeId, InstanceId: p.instanceId, Device: p.device }).pipe(Effect.asVoid, aws("ec2", "AttachVolume", KnownAwsErrors.None)),
          ),

        DetachVolume: (p) => guard.run("DetachVolume", p, undefined, () => ec2.detachVolume({ VolumeId: p.volumeId }).pipe(Effect.asVoid, aws("ec2", "DetachVolume", KnownAwsErrors.None))),

        DeleteVolume: (p) =>
          guard.run("DeleteVolume", p, undefined, () =>
            ec2.deleteVolume({ VolumeId: p.volumeId }).pipe(
              Effect.as("deleted" as const),
              onCode(["InvalidVolume.NotFound"], () => Effect.succeed("gone" as const)),
              onCode(["VolumeInUse"], () => Effect.succeed("in-use" as const)),
              aws("ec2", "DeleteVolume", KnownAwsErrors.None),
            ),
          ),

        ModifyVolume: (p) =>
          guard.run("ModifyVolume", p, undefined, () =>
            ec2.modifyVolume({ VolumeId: p.volumeId, Throughput: p.throughputMbps, Iops: p.iops }).pipe(Effect.asVoid, aws("ec2", "ModifyVolume", KnownAwsErrors.None)),
          ),

        DryRun: (p) => {
          const m = p.mutation;
          const claimed = m._tag === "RunBox" || m._tag === "RunHost" || m._tag === "CreateVolume" ? m.orgId : undefined;
          return guard.run("DryRun", p, claimed, (org) => dryRun(m, org));
        },

        DescribeInstances: (q) =>
          guard.run("DescribeInstances", q, q.orgId, (org) => {
            const where = filters(instanceFilters(org, q));
            const request = q.instanceIds === undefined ? { Filters: where, MaxResults: MAX_LISTED } : { InstanceIds: [...q.instanceIds], Filters: where };
            return describeInstances(request, MAX_LISTED).pipe(
              aws("ec2", "DescribeInstances", KnownAwsErrors.DescribeInstances),
              Effect.map((list) => list.flatMap(instanceOf)),
            );
          }),

        DescribeVolumes: (q) =>
          guard.run("DescribeVolumes", q, q.orgId, (org) =>
            ec2.describeVolumes({ Filters: filters(volumeFilters(org, q)) }).pipe(
              aws("ec2", "DescribeVolumes", KnownAwsErrors.None),
              Effect.map((out) => (out.Volumes ?? []).flatMap(volumeOf).slice(0, MAX_LISTED)),
            ),
          ),

        VolumeModifications: (p) =>
          guard.run("VolumeModifications", p, undefined, (org) =>
            Effect.gen(function* () {
              if (org !== null) {
                const mine = yield* ec2.describeVolumes({ Filters: filters(volumeFilters(org, { orgId: org, volumeIds: [p.volumeId] })) }).pipe(aws("ec2", "DescribeVolumes", KnownAwsErrors.None));
                if ((mine.Volumes ?? []).length === 0) return [];
              }
              const out = yield* ec2.describeVolumesModifications({ VolumeIds: [p.volumeId] }).pipe(
                onCode(["InvalidVolumeModification.NotFound", "InvalidVolume.NotFound"], () => Effect.succeed({} as ec2.DescribeVolumesModificationsResult)),
                aws("ec2", "DescribeVolumesModifications", KnownAwsErrors.None),
              );
              return (out.VolumesModifications ?? [])
                .flatMap((m) =>
                  member(MODIFICATION_STATES, m.ModificationState)
                    ? [{ state: m.ModificationState, startMs: m.StartTime === undefined ? 0 : new Date(m.StartTime).getTime(), targetMbps: m.TargetThroughput ?? null }]
                    : [],
                )
                .slice(0, 100);
            }),
          ),

        VcpuUsage: (p) =>
          guard.run("VcpuUsage", p, p.orgId, (org) =>
            describeInstances({ Filters: [{ Name: "instance-state-name", Values: BURNING }], MaxResults: MAX_LISTED }, Number.POSITIVE_INFINITY).pipe(
              aws("ec2", "DescribeInstances", KnownAwsErrors.None),
              Effect.map((list) =>
                vcpuUsage(
                  org,
                  list.map((i) => ({
                    state: i.State?.Name ?? "",
                    instanceType: i.InstanceType ?? "",
                    vcpus: (i.CpuOptions?.CoreCount ?? 0) * (i.CpuOptions?.ThreadsPerCore ?? 1),
                    spot: i.InstanceLifecycle === "spot",
                    tags: tagsOf(i.Tags),
                  })),
                ),
              ),
            ),
          ),

        VcpuQuota: () =>
          guard.run("VcpuQuota", null, undefined, () =>
            quotas.getServiceQuota({ ServiceCode: "ec2", QuotaCode: VCPU_QUOTA_CODE }).pipe(
              aws("servicequotas", "GetServiceQuota", KnownAwsErrors.None),
              Effect.flatMap((out) =>
                out.Quota?.Value === undefined
                  ? Effect.fail(new AwsUnanswered({ service: "servicequotas", action: "GetServiceQuota", message: "GetServiceQuota answered without a value" }))
                  : Effect.succeed({ vcpus: Math.floor(out.Quota.Value) }),
              ),
            ),
          ),

        DescribeSubnets: () =>
          guard.run("DescribeSubnets", null, undefined, () =>
            ec2.describeSubnets({ SubnetIds: [...config.subnetIds] }).pipe(
              aws("ec2", "DescribeSubnets", KnownAwsErrors.None),
              Effect.map((out) =>
                (out.Subnets ?? []).flatMap((s) =>
                  s.SubnetId === undefined || s.VpcId === undefined
                    ? []
                    : [
                        {
                          subnetId: s.SubnetId,
                          vpcId: s.VpcId,
                          zone: s.AvailabilityZone ?? null,
                          zoneId: s.AvailabilityZoneId ?? null,
                          cidrBlock: s.CidrBlock ?? null,
                          mapPublicIpOnLaunch: s.MapPublicIpOnLaunch === true,
                        },
                      ],
                ),
              ),
            ),
          ),

        DescribeSecurityGroup: () =>
          guard.run("DescribeSecurityGroup", null, undefined, () =>
            ec2.describeSecurityGroups({ GroupIds: [config.securityGroupId] }).pipe(
              Effect.map((out) => out.SecurityGroups?.[0]),
              onCode(["InvalidGroup.NotFound"], () => Effect.succeed(undefined)),
              aws("ec2", "DescribeSecurityGroups", KnownAwsErrors.None),
              Effect.map((g) =>
                g?.GroupId === undefined || g.VpcId === undefined
                  ? null
                  : {
                      groupId: g.GroupId,
                      vpcId: g.VpcId,
                      ingress: (g.IpPermissions ?? []).map((r) => ({
                        protocol: r.IpProtocol ?? "-1",
                        fromPort: r.FromPort ?? null,
                        toPort: r.ToPort ?? null,
                        cidrs: [...(r.IpRanges ?? []).flatMap((x) => x.CidrIp ?? []), ...(r.Ipv6Ranges ?? []).flatMap((x) => x.CidrIpv6 ?? [])],
                        groupIds: (r.UserIdGroupPairs ?? []).flatMap((x) => x.GroupId ?? []),
                        prefixLists: (r.PrefixListIds ?? []).flatMap((x) => x.PrefixListId ?? []),
                      })),
                    },
              ),
            ),
          ),

        DescribeRouteTables: () =>
          guard.run("DescribeRouteTables", null, undefined, () =>
            ec2.describeRouteTables({ Filters: [{ Name: "vpc-id", Values: [config.vpcId] }] }).pipe(
              aws("ec2", "DescribeRouteTables", KnownAwsErrors.DescribeRouteTables),
              Effect.map((out) =>
                (out.RouteTables ?? []).slice(0, 200).map((t) => ({
                  routeTableId: t.RouteTableId ?? "",
                  main: (t.Associations ?? []).some((a) => a.Main === true),
                  subnetIds: (t.Associations ?? []).flatMap((a) => a.SubnetId ?? []),
                  routes: (t.Routes ?? []).map((r) => ({
                    destination: r.DestinationCidrBlock ?? r.DestinationIpv6CidrBlock ?? r.DestinationPrefixListId ?? "",
                    target: r.GatewayId ?? r.NatGatewayId ?? r.TransitGatewayId ?? r.NetworkInterfaceId ?? r.InstanceId ?? r.VpcPeeringConnectionId ?? r.EgressOnlyInternetGatewayId ?? "",
                  })),
                })),
              ),
            ),
          ),

        FederationInfo: () =>
          guard.run("FederationInfo", null, undefined, () =>
            iam.getOutboundWebIdentityFederationInfo({ region: "us-east-1" }).pipe(
              Effect.map((out) => ({ enabled: out.JwtVendingEnabled === true })),
              onCode(["FeatureDisabledException"], () => Effect.succeed({ enabled: false })),
              aws("iam", "GetOutboundWebIdentityFederationInfo", KnownAwsErrors.None),
            ),
          ),

        PutParameter: (p) =>
          guard.run("PutParameter", { name: p.name }, undefined, () =>
            Effect.andThen(
              parameters,
              Semaphore.withPermits(
                writing,
                1,
              )(ssm.putParameter({ Name: p.name, Value: p.value, Type: "SecureString", Overwrite: true }).pipe(Effect.asVoid, aws("ssm", "PutParameter", KnownAwsErrors.None))),
            ),
          ),

        DeleteParameters: (p) =>
          guard.run("DeleteParameters", p, undefined, () =>
            Effect.andThen(parameters, Semaphore.withPermits(writing, 1)(ssm.deleteParameters({ Names: [...p.names] }).pipe(Effect.asVoid, aws("ssm", "DeleteParameters", KnownAwsErrors.None)))),
          ),

        DeleteChunks: (p) =>
          guard.run("DeleteChunks", p, undefined, () =>
            Effect.gen(function* () {
              const { config: c, bucket: name } = yield* bucket;
              const keys = chunkKeys(c, p.ids);
              const idOf = new Map(keys.map((k, i) => [k, p.ids[i]!]));
              const out = yield* s3.deleteObjects({ Bucket: name, Delete: { Objects: keys.map((Key) => ({ Key })), Quiet: true } }).pipe(aws("s3", "DeleteObjects", KnownAwsErrors.None));
              const refused = (out.Errors ?? []).flatMap((e) => {
                const id = e.Key === undefined ? undefined : idOf.get(e.Key);
                return id === undefined ? [] : [{ id, message: `${e.Code ?? "Error"}: ${e.Message ?? ""}`.slice(0, 300) }];
              });
              const bad = new Set(refused.map((r) => r.id));
              return { removed: p.ids.filter((id) => !bad.has(id)), refused };
            }),
          ),

        ListChunks: () =>
          guard.run("ListChunks", null, undefined, () =>
            Effect.gen(function* () {
              const { config: c, bucket: name } = yield* bucket;
              const out = yield* s3.listObjectsV2({ Bucket: name, Prefix: chunkPrefix(c.orgId), MaxKeys: 1 }).pipe(aws("s3", "ListObjectsV2", KnownAwsErrors.None));
              return { any: (out.KeyCount ?? out.Contents?.length ?? 0) > 0 };
            }),
          ),

        BucketVersioning: () =>
          guard.run("BucketVersioning", null, undefined, () =>
            Effect.gen(function* () {
              const { bucket: name } = yield* bucket;
              const out = yield* s3.getBucketVersioning({ Bucket: name }).pipe(aws("s3", "GetBucketVersioning", KnownAwsErrors.None));
              return { status: out.Status === "Enabled" ? ("Enabled" as const) : out.Status === "Suspended" ? ("Suspended" as const) : ("" as const) };
            }),
          ),

        BucketLifecycle: () =>
          guard.run("BucketLifecycle", null, undefined, () =>
            Effect.gen(function* () {
              const { bucket: name } = yield* bucket;
              const rules = yield* s3.getBucketLifecycleConfiguration({ Bucket: name }).pipe(
                Effect.map((out) => out.Rules ?? []),
                onCode(["NoSuchLifecycleConfiguration"], () => Effect.succeed([])),
                aws("s3", "GetBucketLifecycleConfiguration", KnownAwsErrors.None),
              );
              return { expiresNoncurrent: rules.some((r) => r.Status === "Enabled" && r.NoncurrentVersionExpiration?.NoncurrentDays !== undefined) };
            }),
          ),

        ProbeVersionDelete: () =>
          guard.run("ProbeVersionDelete", null, undefined, () =>
            Effect.gen(function* () {
              const { config: c, bucket: name } = yield* bucket;
              yield* s3.deleteObject({ Bucket: name, Key: chunkKey(c.orgId, PROBE_CHUNK), VersionId: "null" }).pipe(aws("s3", "DeleteObject", KnownAwsErrors.ProbeVersionDelete));
            }),
          ),

        EnsureOrgSecurityGroup: (p) =>
          guard.run("EnsureOrgSecurityGroup", p, p.orgId, (org) =>
            Effect.gen(function* () {
              const c = yield* hosted;
              const group = orgGroup(c, org);
              const groupId = yield* ec2
                .createSecurityGroup({ GroupName: group.name, Description: `tesser org ${org}`, VpcId: c.vpcId, TagSpecifications: [{ ResourceType: "security-group", Tags: tagList(group.tags) }] })
                .pipe(
                  Effect.map((out) => out.GroupId),
                  onCode(["InvalidGroup.Duplicate"], () =>
                    ec2
                      .describeSecurityGroups({
                        Filters: [
                          { Name: "vpc-id", Values: [c.vpcId] },
                          { Name: "group-name", Values: [group.name] },
                        ],
                      })
                      .pipe(Effect.map((out) => out.SecurityGroups?.[0]?.GroupId)),
                  ),
                  aws("ec2", "CreateSecurityGroup", KnownAwsErrors.None),
                );
              if (groupId === undefined) return yield* new AwsUnanswered({ service: "ec2", action: "CreateSecurityGroup", message: `security group ${group.name} has no id` });
              for (const rule of group.rules(groupId)) {
                yield* ec2
                  .authorizeSecurityGroupIngress({
                    GroupId: groupId,
                    IpPermissions: [
                      {
                        IpProtocol: rule.protocol,
                        FromPort: rule.fromPort,
                        ToPort: rule.toPort,
                        ...("cidr" in rule.from ? { IpRanges: [{ CidrIp: rule.from.cidr }] } : { UserIdGroupPairs: [{ GroupId: rule.from.groupId }] }),
                      },
                    ],
                  })
                  .pipe(
                    Effect.asVoid,
                    onCode(["InvalidPermission.Duplicate"], () => Effect.void),
                    aws("ec2", "AuthorizeSecurityGroupIngress", KnownAwsErrors.None),
                  );
              }
              return { groupId };
            }),
          ),

        DeleteSecurityGroup: (p) =>
          guard.run("DeleteSecurityGroup", p, undefined, () =>
            ec2.deleteSecurityGroup({ GroupId: p.groupId }).pipe(
              Effect.as("deleted" as const),
              onCode(["InvalidGroup.NotFound"], () => Effect.succeed("deleted" as const)),
              onCode(["DependencyViolation"], () => Effect.succeed("in-use" as const)),
              aws("ec2", "DeleteSecurityGroup", KnownAwsErrors.None),
            ),
          ),

        CostAndUsage: (p) =>
          guard.run("CostAndUsage", p, undefined, () =>
            Effect.gen(function* () {
              const days: { day: string; estimated: boolean; services: { service: string; amount: number }[] }[] = [];
              let token: string | undefined;
              do {
                const page: ce.GetCostAndUsageResponse = yield* ce
                  .getCostAndUsage({
                    region: "us-east-1",
                    TimePeriod: { Start: p.start, End: p.end },
                    Granularity: "DAILY",
                    Metrics: ["UnblendedCost"],
                    GroupBy: [{ Type: "DIMENSION", Key: "SERVICE" }],
                    ...(token === undefined ? {} : { NextPageToken: token }),
                  })
                  .pipe(aws("ce", "GetCostAndUsage", KnownAwsErrors.None));
                for (const slot of page.ResultsByTime ?? []) {
                  if (slot.TimePeriod === undefined) continue;
                  const services = (slot.Groups ?? []).flatMap((g) => {
                    const amount = Number(g.Metrics?.UnblendedCost?.Amount ?? 0);
                    return Number.isFinite(amount) && amount !== 0 ? [{ service: (g.Keys?.[0] ?? "Other").slice(0, 256), amount }] : [];
                  });
                  days.push({ day: slot.TimePeriod.Start, estimated: slot.Estimated === true, services: services.slice(0, 500) });
                }
                token = page.NextPageToken === "" ? undefined : page.NextPageToken;
              } while (token !== undefined && days.length < 400);
              return days.slice(0, 400);
            }),
          ),

        InstanceTypeOfferings: (p) =>
          guard.run("InstanceTypeOfferings", p, undefined, () =>
            Effect.gen(function* () {
              const types: string[] = [];
              let token: string | undefined;
              do {
                const page: ec2.DescribeInstanceTypeOfferingsResult = yield* ec2
                  .describeInstanceTypeOfferings({
                    region: p.region,
                    LocationType: "availability-zone-id",
                    Filters: [
                      { Name: "location", Values: [p.zoneId] },
                      { Name: "instance-type", Values: [...p.instanceTypes] },
                    ],
                    ...(token === undefined ? {} : { NextToken: token }),
                  })
                  .pipe(aws("ec2", "DescribeInstanceTypeOfferings", KnownAwsErrors.None));
                types.push(...(page.InstanceTypeOfferings ?? []).flatMap((o) => o.InstanceType ?? []));
                token = page.NextToken === "" ? undefined : page.NextToken;
              } while (token !== undefined && types.length < 100);
              return types.slice(0, 100);
            }),
          ),

        ImagesCredentials: (p) =>
          guard.run("ImagesCredentials", p, undefined, () =>
            Effect.gen(function* () {
              const session = yield* Effect.fromResult(imagesSession(yield* hosted, p.region, p.hostId, p.keys));
              const out = yield* sts.assumeRole(session).pipe(aws("sts", "AssumeRole", KnownAwsErrors.None));
              return yield* granted("AssumeRole", out.Credentials);
            }),
          ),

        OrgS3Credentials: (p) =>
          guard.run("OrgS3Credentials", p, p.orgId, (org) =>
            Effect.gen(function* () {
              const session = orgS3Session(yield* hosted, org);
              const out = yield* sts.assumeRole(session).pipe(aws("sts", "AssumeRole", KnownAwsErrors.None));
              return yield* granted("AssumeRole", out.Credentials);
            }),
          ),
      });
    }),
  );
