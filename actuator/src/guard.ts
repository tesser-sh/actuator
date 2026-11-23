import { actingOrg, admit, served } from "@tesser/protocol/actuator-fence";
import { OPS, type ActuatorConfig, type ActuatorOp, type FenceRefused, type RateLimited } from "@tesser/protocol/actuator-rpc";
import { Clock, Effect, Exit, Fiber, FiberSet, Option, Result } from "effect";
import type { Audit } from "./audit.ts";

const MINUTE_MS = 60_000;
const HOUR_MS = 3_600_000;
const LAUNCHES: ReadonlySet<ActuatorOp> = new Set(["RunBox", "RunHost"]);

type Refusal = { readonly _tag: string; readonly message?: string | undefined };

interface Health {
  readonly startedAt: number;
  readonly calls: number;
  readonly refused: number;
  readonly limited: number;
  readonly lastError: string | null;
}

export interface Guard {
  readonly run: <O extends string | null, A, E extends Refusal, R>(
    op: ActuatorOp,
    request: unknown,
    org: O | undefined,
    body: (org: O) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | FenceRefused | RateLimited, R>;
  readonly health: Effect.Effect<Health>;
  readonly busy: Effect.Effect<readonly ActuatorOp[]>;
  readonly idle: Effect.Effect<void>;
}

const told = (op: ActuatorOp, e: Refusal) => `${op}: ${e._tag}${e.message === undefined || e.message === "" ? "" : `: ${e.message}`}`.slice(0, 1000);

export const makeGuard = Effect.fn("Guard.make")(function* (config: ActuatorConfig, audit: Audit) {
  const work = yield* FiberSet.make();
  const startedAt = yield* Clock.currentTimeMillis;
  const home = config.kind === "selfHost" ? config.orgId : null;
  const running = new Map<number, ActuatorOp>();
  let mutations: number[] = [];
  let launches: number[] = [];
  let calls = 0;
  let refused = 0;
  let limited = 0;
  let lastError: string | null = null;
  let next = 0;

  const admitted = (op: ActuatorOp, now: number): Result.Result<void, RateLimited> => {
    if (OPS[op].audit !== "mutation") return Result.succeed(undefined);
    const minute = admit(mutations, now, config.limits.mutationsPerMinute, MINUTE_MS);
    if (Result.isFailure(minute)) return Result.fail(minute.failure);
    const hour = LAUNCHES.has(op) ? admit(launches, now, config.limits.launchesPerHour, HOUR_MS) : Result.succeed(launches);
    if (Result.isFailure(hour)) return Result.fail(hour.failure);
    mutations = minute.success;
    launches = hour.success;
    return Result.succeed(undefined);
  };

  const failed = (op: ActuatorOp, exit: Exit.Exit<unknown, Refusal>) => {
    if (Exit.isSuccess(exit)) return;
    refused++;
    const error = Exit.findErrorOption(exit);
    if (Option.isSome(error) && error.value._tag === "RateLimited") limited++;
    lastError = Option.match(error, { onNone: () => `${op}: Defect`, onSome: (e) => told(op, e) });
  };

  const run = <O extends string | null, A, E extends Refusal, R>(
    op: ActuatorOp,
    request: unknown,
    claimed: O | undefined,
    body: (org: O) => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | FenceRefused | RateLimited, R> =>
    Effect.gen(function* () {
      const at = yield* Clock.currentTimeMillis;
      calls++;
      const acting = Result.flatMap(served(config, op), () => (claimed === undefined ? Result.succeed(home) : actingOrg(config, claimed)));
      const org = (Result.isSuccess(acting) ? acting.success : (claimed ?? home)) as O;
      const gate = Result.flatMap(acting, () => admitted(op, at));
      if (Result.isFailure(gate)) {
        const exit = Exit.fail(gate.failure);
        failed(op, exit);
        yield* audit.record(op, at, org, request, exit);
        return yield* Effect.fail(gate.failure);
      }
      const id = next++;
      running.set(id, op);
      const fiber = yield* FiberSet.run(
        work,
        Effect.exit(body(org)).pipe(
          Effect.tap((exit) => {
            running.delete(id);
            failed(op, exit);
            return audit.record(op, at, org, request, exit);
          }),
        ),
      );
      return yield* Effect.flatten(Fiber.join(fiber));
    });

  return {
    run,
    health: Effect.sync(() => ({ startedAt, calls, refused, limited, lastError })),
    busy: Effect.sync(() => [...running.values()]),
    idle: FiberSet.awaitEmpty(work),
  } satisfies Guard;
});
