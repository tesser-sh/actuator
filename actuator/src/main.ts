import * as Credentials from "@distilled.cloud/aws/Credentials";
import * as Region from "@distilled.cloud/aws/Region";
import * as sts from "@distilled.cloud/aws/sts";
import { BunSocket } from "@effect/platform-bun";
import { ActuatorConfig, actuatorRoleArn, KnownAwsErrors } from "@tesser/protocol/actuator-rpc";
import { Clock, Deferred, Duration, Effect, Exit, Fiber, Layer, Logger, Option, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpLogger, OtlpSerialization, OtlpTracer } from "effect/observability";
import { makeAudit } from "./audit.ts";
import { aws } from "./aws.ts";
import { makeGuard } from "./guard.ts";
import { runLink } from "./link.ts";
import { makeOps } from "./ops.ts";

declare const __ACTUATOR_VERSION__: string | undefined;
const VERSION = typeof __ACTUATOR_VERSION__ === "string" ? __ACTUATOR_VERSION__ : "0.0.0";
const DRAIN_LIMIT = Duration.seconds(110);
const FLUSH_EVERY = Duration.minutes(1);
const DATADOG_OTLP = "https://otlp.datadoghq.com/v1";

class StartFailed extends Schema.TaggedError<StartFailed>()("StartFailed", { message: Schema.String }) {}

const stdout = (line: string) => Effect.promise(() => Bun.write(Bun.stdout, `${line}\n`)).pipe(Effect.asVoid);

const datadog = (config: ActuatorConfig) => {
  const key = process.env.DD_API_KEY;
  if (config.kind === "selfHost" || key === undefined || key === "") return Layer.empty;
  const resource = {
    serviceName: "tesser-actuator",
    serviceVersion: VERSION,
    attributes: { "deployment.environment.name": "prod", "cloud.provider": "aws", "cloud.account.id": config.accountId, "cloud.region": config.region },
  };
  const headers = { "dd-api-key": key };
  return Layer.mergeAll(
    OtlpTracer.layer({ url: `${DATADOG_OTLP}/traces`, resource, headers: { ...headers, compute_stats: "true" }, compression: "gzip" }),
    OtlpLogger.layer({ url: `${DATADOG_OTLP}/logs`, resource, headers, compression: "gzip" }),
  ).pipe(Layer.provide(Layer.mergeAll(OtlpSerialization.layerProtobuf, FetchHttpClient.layer)));
};

const serve = Effect.fn("Actuator.serve")(function* (config: ActuatorConfig, draining: Deferred.Deferred<void>) {
  const caller = yield* sts.getCallerIdentity({}).pipe(
    aws("sts", "GetCallerIdentity", KnownAwsErrors.None),
    Effect.mapError((e) => new StartFailed({ message: `GetCallerIdentity: ${e.message}` })),
  );
  if (caller.Account !== config.accountId) return yield* new StartFailed({ message: `this task runs in account ${caller.Account ?? "unknown"}, and its config says ${config.accountId}` });
  yield* Effect.log(`actuator ${VERSION} for ${config.kind === "selfHost" ? config.orgId : "tesser's hosted fleet"} in ${config.accountId} ${config.region} as ${caller.Arn ?? "unknown"}`);
  const audit = yield* makeAudit(stdout);
  const guard = yield* makeGuard(config, audit);
  const handlers = yield* Layer.build(makeOps(config, guard));
  yield* Effect.forkScoped(Effect.sleep(FLUSH_EVERY).pipe(Effect.andThen(Effect.flatMap(Clock.currentTimeMillis, audit.flush)), Effect.forever));
  const link = yield* Effect.forkScoped(runLink({ config, version: VERSION, role: actuatorRoleArn(caller.Arn), guard, handlers, draining }));
  yield* Deferred.await(draining);
  yield* Effect.log("draining");
  const settled = yield* Fiber.join(link).pipe(Effect.andThen(guard.idle), Effect.timeoutOption(DRAIN_LIMIT));
  if (Option.isNone(settled)) yield* Effect.logWarning(`stopping with ${(yield* guard.busy).join(", ") || "the cell link"} still running after ${Duration.format(DRAIN_LIMIT)}`);
  yield* audit.flush(Number.POSITIVE_INFINITY);
});

const program = Effect.gen(function* () {
  const draining = yield* Deferred.make<void>();
  const stop = () => void Effect.runFork(Deferred.succeed(draining, undefined));
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  const config = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ActuatorConfig))(process.env.ACTUATOR_CONFIG ?? "").pipe(
    Effect.mapError((e) => new StartFailed({ message: `ACTUATOR_CONFIG: ${e.message}` })),
  );
  const region = process.env.AWS_REGION;
  if (region !== undefined && region !== config.region) return yield* new StartFailed({ message: `this task runs in ${region}, and its config says ${config.region}` });
  yield* serve(config, draining).pipe(Effect.provide(Layer.mergeAll(Credentials.fromContainerMetadata(), Region.of(config.region), FetchHttpClient.layer, datadog(config))));
});

const fiber = Effect.runFork(
  program.pipe(
    Effect.tapCause((cause) => Effect.logError(cause)),
    Effect.scoped,
    Effect.provide(Layer.mergeAll(BunSocket.layerWebSocketConstructor, Logger.layer([Logger.withConsoleError(Logger.formatJson)]))),
  ),
);
fiber.addObserver((exit) => process.exit(Exit.isSuccess(exit) ? 0 : 1));
