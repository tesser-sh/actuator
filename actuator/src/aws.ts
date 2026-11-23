import { AwsUnanswered, refusal, type AwsErrorClass, type AwsRefused } from "@tesser/protocol/actuator-rpc";
import { Duration, Effect, Redacted } from "effect";

const AWS_TIMEOUT = Duration.seconds(30);
const UNANSWERED = new Set([
  "ParseError",
  "EndpointError",
  "NoMatchingRuleError",
  "InvalidInput",
  "SdkError",
  "HttpClientError",
  "TransientFetchError",
  "SigningError",
  "CryptoError",
  "XmlParseError",
  "ConfigError",
  "SourceError",
]);

type Service = AwsUnanswered["service"];
type Answer = { readonly _tag: string; readonly message?: string | undefined };

const answered = (e: unknown): e is Answer => typeof e === "object" && e !== null && "_tag" in e && typeof e._tag === "string" && !UNANSWERED.has(e._tag) && !e._tag.includes("::");

const messageOf = (e: unknown): string => {
  if (typeof e === "object" && e !== null && "message" in e && typeof e.message === "string" && e.message !== "") return e.message;
  if (typeof e === "object" && e !== null && "_tag" in e && typeof e._tag === "string") return e._tag;
  return String(e);
};

export const awsCode = (e: unknown): string | null => {
  if (!answered(e)) return null;
  if (e._tag !== "UnknownAwsError") return e._tag;
  const code = (e as { code?: unknown }).code;
  return typeof code === "string" ? code : null;
};

export const aws =
  <const K extends readonly AwsErrorClass[]>(service: Service, action: string, known: K) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, InstanceType<K[number]> | AwsRefused | AwsUnanswered, R> =>
    effect.pipe(
      Effect.timeoutOrElse({ duration: AWS_TIMEOUT, orElse: () => Effect.fail(new AwsUnanswered({ service, action, message: `no answer in ${Duration.format(AWS_TIMEOUT)}` })) }),
      Effect.mapError((e) => (e instanceof AwsUnanswered ? e : answered(e) ? refusal(known)(e) : new AwsUnanswered({ service, action, message: messageOf(e).slice(0, 1000) }))),
      Effect.withSpan(`aws.${service}.${action}`, { kind: "client", attributes: { "rpc.system": "aws-api", "rpc.service": service, "rpc.method": action } }),
    );

export const onCode =
  <B, E2, R2>(codes: readonly string[], f: (e: unknown) => Effect.Effect<B, E2, R2>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A | B, E | E2, R | R2> =>
    Effect.catchIf(effect, (e) => codes.includes(awsCode(e) ?? ""), f);

export const plain = (v: string | Redacted.Redacted<string>): string => (Redacted.isRedacted(v) ? Redacted.value(v) : v);

export const tagList = (tags: Readonly<Record<string, string>>) => Object.entries(tags).map(([Key, Value]) => ({ Key, Value }));

export const tagsOf = (tags: readonly { Key?: string | undefined; Value?: string | undefined }[] | undefined): Record<string, string> =>
  Object.fromEntries((tags ?? []).flatMap((t) => (t.Key === undefined ? [] : [[t.Key, t.Value ?? ""]])));
