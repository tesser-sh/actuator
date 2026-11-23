import * as sts from "@distilled.cloud/aws/sts";
import { served } from "@tesser/protocol/actuator-fence";
import { ACTUATOR_PING, ACTUATOR_PONG, ActuatorCellRpcs, ActuatorRpcs, actuatorUrl, AwsUnanswered, KnownAwsErrors, OPS, type ActuatorConfig, type ActuatorOp } from "@tesser/protocol/actuator-rpc";
import { makeWire } from "@tesser/protocol/wire";
import { Clock, type Context, Deferred, Duration, Effect, Option, Queue, Result, Schema } from "effect";
import { HttpClient } from "effect/http";
import { type Rpc, RpcClient, type RpcGroup, RpcServer } from "effect/rpc";
import { Socket } from "effect/socket";
import { aws, plain } from "./aws.ts";
import type { Guard } from "./guard.ts";

const IDENTITY_TTL_S = 300;
const HELLO_TIMEOUT = Duration.seconds(30);
const DRAINING_TIMEOUT = Duration.seconds(10);
const HEALTH_EVERY = Duration.seconds(60);
const PING_EVERY = Duration.seconds(15);
const PONG_DEADLINE_MS = 45_000;
const SETTLE = Duration.seconds(1);
const REFUSAL_TIMEOUT = Duration.seconds(10);
const BASE_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 30_000;

export interface Link {
  readonly config: ActuatorConfig;
  readonly version: string;
  readonly guard: Guard;
  readonly handlers: Context.Context<Rpc.ToHandler<RpcGroup.Rpcs<typeof ActuatorRpcs>>>;
  readonly draining: Deferred.Deferred<void>;
}

const decoder = new TextDecoder();

export const servedOps = (config: ActuatorConfig) => (Object.keys(OPS) as ActuatorOp[]).filter((op) => Result.isSuccess(served(config, op)));

const identity = (config: ActuatorConfig) =>
  sts.getWebIdentityToken({ Audience: [config.cellUrl], SigningAlgorithm: "RS256", DurationSeconds: IDENTITY_TTL_S }).pipe(
    aws("sts", "GetWebIdentityToken", KnownAwsErrors.None),
    Effect.flatMap((out) =>
      out.WebIdentityToken === undefined
        ? Effect.fail(new AwsUnanswered({ service: "sts", action: "GetWebIdentityToken", message: "GetWebIdentityToken answered without a token" }))
        : Effect.succeed(plain(out.WebIdentityToken)),
    ),
  );

const Refusal = Schema.fromJsonString(Schema.Struct({ error: Schema.Struct({ message: Schema.String }) }));

const refusal = (url: string) =>
  HttpClient.get(url.replace(/^wss:/, "https:")).pipe(
    Effect.flatMap((res) =>
      Effect.map(res.text, (body) => {
        const message = Schema.decodeUnknownOption(Refusal)(body).pipe(
          Option.map((r) => r.error.message),
          Option.getOrUndefined,
        );
        return message === "websocket upgrade required" ? undefined : `${res.status}${message === undefined ? "" : ` ${message}`}`;
      }),
    ),
    Effect.timeout(REFUSAL_TIMEOUT),
    Effect.orElseSucceed(() => undefined),
  );

const session = (link: Link, greeted: () => Effect.Effect<void>) =>
  Effect.gen(function* () {
    const { config, guard, draining } = link;
    const url = actuatorUrl(config);
    const token = yield* Effect.result(identity(config));
    if (Result.isFailure(token)) return { greeted: false, why: `no identity token: ${token.failure._tag}: ${token.failure.message}` };
    const socket = yield* Socket.makeWebSocket(url);
    const opened = yield* Effect.result(socket.reader);
    if (Result.isFailure(opened)) {
      const refused = yield* refusal(url);
      return { greeted: false, why: refused === undefined ? `cannot reach ${url}: ${opened.failure.message}` : `${url} refused: ${refused}` };
    }
    const reader = opened.success;
    const writer = yield* socket.writer;
    const out = yield* Queue.unbounded<string | Socket.CloseEvent>();
    yield* Effect.addFinalizer(() => Queue.shutdown(out));
    const wire = yield* makeWire((frame) => Queue.offer(out, frame), { spanParents: true });
    const cell = yield* RpcClient.make(ActuatorCellRpcs).pipe(Effect.provideService(RpcClient.Protocol, wire.client));
    yield* RpcServer.make(ActuatorRpcs, { disableFatalDefects: true }).pipe(Effect.provide(link.handlers), Effect.provideService(RpcServer.Protocol, wire.server), Effect.forkScoped);
    let said = false;
    let ponged = 0;

    const write = Queue.take(out).pipe(
      Effect.flatMap(writer.write),
      Effect.forever,
      Effect.catchTag("SocketError", (err) => Effect.as(wire.closed(err.reason), err.message)),
    );
    const read = Effect.forever(
      Effect.gen(function* () {
        for (const frame of yield* reader.pull) {
          const text = typeof frame === "string" ? frame : decoder.decode(frame);
          if (text === ACTUATOR_PONG) ponged = yield* Clock.currentTimeMillis;
          else if (!(yield* wire.receive(text))) yield* Effect.logWarning("dropped a malformed frame from the cell");
        }
      }),
    ).pipe(
      Effect.catchTag("SocketError", (err) =>
        Effect.as(wire.closed(err.reason), err.reason._tag === "SocketCloseError" ? `closed ${err.reason.code} ${err.reason.closeReason ?? ""}`.trim() : err.message),
      ),
    );

    const drain = Deferred.await(draining).pipe(
      Effect.andThen(Effect.ignore(cell.Draining().pipe(Effect.timeout(DRAINING_TIMEOUT)))),
      Effect.andThen(Effect.sleep(SETTLE)),
      Effect.andThen(guard.idle),
      Effect.andThen(Effect.sleep(SETTLE)),
      Effect.andThen(Queue.offer(out, new Socket.CloseEvent(1000, "drained"))),
    );
    const beat = Effect.gen(function* () {
      for (;;) {
        yield* Effect.sleep(PING_EVERY);
        if ((yield* Clock.currentTimeMillis) - ponged > PONG_DEADLINE_MS) return `no pong from the cell in ${PONG_DEADLINE_MS / 1000}s`;
        yield* Queue.offer(out, ACTUATOR_PING);
      }
    });
    const health = Effect.sleep(HEALTH_EVERY).pipe(Effect.andThen(Effect.flatMap(guard.health, (h) => Effect.ignore(cell.Health(h)))), Effect.forever);

    const talk = Effect.gen(function* () {
      const answer = yield* cell
        .Hello({ version: link.version, ops: servedOps(config), config, identity: token.success })
        .pipe(Effect.timeoutOrElse({ duration: HELLO_TIMEOUT, orElse: () => Effect.fail({ _tag: "Timeout", message: `no answer to hello in ${Duration.format(HELLO_TIMEOUT)}` }) }), Effect.result);
      if (Result.isFailure(answer)) return answer.failure._tag === "HelloRefused" ? `refused: ${answer.failure.message}` : `hello failed: ${answer.failure.message}`;
      said = true;
      ponged = yield* Clock.currentTimeMillis;
      yield* greeted();
      yield* Effect.forkScoped(health);
      yield* Effect.forkScoped(drain);
      return yield* beat;
    });

    const why = yield* Effect.raceFirst(read, Effect.raceFirst(write, talk));
    return { greeted: said, why };
  }).pipe(Effect.scoped);

export const runLink = Effect.fn("Link.run")(function* (link: Link) {
  let state = "";
  let attempts = 0;
  const note = (next: string, warn: boolean) =>
    next === state
      ? Effect.void
      : Effect.suspend(() => {
          state = next;
          return warn ? Effect.logWarning(next) : Effect.log(next);
        });
  while (!(yield* Deferred.isDone(link.draining))) {
    const { greeted, why } = yield* session(link, () => note(`connected to ${actuatorUrl(link.config)}`, false));
    if (greeted) attempts = 0;
    if (yield* Deferred.isDone(link.draining)) break;
    yield* note(why, true);
    const wait = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** attempts++);
    yield* Effect.raceFirst(Effect.sleep(Duration.millis(wait)), Deferred.await(link.draining));
  }
});
