import { auditLine } from "@tesser/protocol/actuator-fence";
import { OPS, type ActuatorOp, type AuditLine } from "@tesser/protocol/actuator-rpc";
import { Effect, Exit, Semaphore } from "effect";

const MINUTE_MS = 60_000;

export interface Audit {
  readonly record: (
    op: ActuatorOp,
    at: number,
    orgId: string | null,
    request: unknown,
    exit: Exit.Exit<unknown, { readonly _tag: string; readonly message?: string | undefined }>,
  ) => Effect.Effect<void>;
  readonly flush: (before: number) => Effect.Effect<void>;
}

export const makeAudit = Effect.fn("Audit.make")(function* (write: (line: string) => Effect.Effect<void>) {
  const lock = yield* Semaphore.make(1);
  const reads = new Map<string, { minute: number; op: ActuatorOp; calls: number; refused: number }>();
  const emit = (line: AuditLine) => Semaphore.withPermits(lock, 1)(write(JSON.stringify(line)));

  const record = Effect.fn("Audit.record")(function* (
    op: ActuatorOp,
    at: number,
    orgId: string | null,
    request: unknown,
    exit: Exit.Exit<unknown, { readonly _tag: string; readonly message?: string | undefined }>,
  ) {
    if (OPS[op].audit === "read") {
      const minute = Math.floor(at / MINUTE_MS) * MINUTE_MS;
      const key = `${minute} ${op}`;
      const count = reads.get(key) ?? { minute, op, calls: 0, refused: 0 };
      reads.set(key, { ...count, calls: count.calls + 1, refused: count.refused + (Exit.isFailure(exit) ? 1 : 0) });
    }
    const line = auditLine(op, at, orgId, request, exit);
    if (line !== null) yield* emit(line);
  });

  const flush = Effect.fn("Audit.flush")(function* (before: number) {
    const done = [...reads.entries()].filter(([, r]) => r.minute + MINUTE_MS <= before);
    for (const [key, r] of done) {
      reads.delete(key);
      yield* emit({ _tag: "Reads", ...r });
    }
  });

  return { record, flush } satisfies Audit;
});
