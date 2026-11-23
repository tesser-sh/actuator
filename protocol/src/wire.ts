import { Effect, Queue, Schema } from "effect";
import { RpcClient, RpcClientError, RpcSerialization, RpcServer, type RpcMessage } from "effect/rpc";
import { Socket } from "effect/socket";

const isFrame = Schema.is(
  Schema.Union([
    Schema.Struct({ to: Schema.Literal("server"), m: Schema.Struct({ _tag: Schema.Literals(["Request", "Ack", "Interrupt", "Ping", "Eof"]) }) }),
    Schema.Struct({ to: Schema.Literal("client"), m: Schema.Struct({ _tag: Schema.Literals(["Chunk", "Exit", "Defect", "Pong"]) }) }),
  ]),
);

export interface Wire {
  readonly server: RpcServer.Protocol["Service"];
  readonly client: RpcClient.Protocol["Service"];
  readonly receive: (frame: string) => Effect.Effect<boolean>;
  readonly closed: (reason: Socket.SocketError["reason"]) => Effect.Effect<void>;
}

export const makeWire = Effect.fn("Wire.make")(function* (send: (frame: string) => Effect.Effect<void, Socket.SocketError>, options?: { readonly spanParents?: boolean }) {
  const disconnects = yield* Queue.unbounded<number>();
  let toServer: (clientId: number, msg: RpcMessage.FromClientEncoded) => Effect.Effect<void> = () => Effect.void;
  let toClient: (clientId: number, msg: RpcMessage.FromServerEncoded) => Effect.Effect<void> = () => Effect.void;
  let clients: ReadonlySet<number> = new Set();
  const frame = (to: "server" | "client", m: unknown) => JSON.stringify({ to, m });
  const server = yield* RpcServer.Protocol.make((write) => {
    toServer = write;
    return Effect.succeed({
      disconnects,
      send: (_clientId: number, response: RpcMessage.FromServerEncoded) => Effect.ignore(send(frame("client", response))),
      end: () => Effect.void,
      clientIds: Effect.succeed(new Set([0])),
      initialMessage: Effect.succeedNone,
      supportsAck: false,
      supportsTransferables: false,
      supportsSpanPropagation: options?.spanParents === true,
      supportsNotifications: false,
      codecFor: RpcSerialization.json.codecFor,
    });
  });
  const client = yield* RpcClient.Protocol.make((write, ids) => {
    toClient = write;
    clients = ids;
    return Effect.succeed({
      send: (_clientId: number, request: RpcMessage.FromClientEncoded) => send(frame("server", request)).pipe(Effect.mapError((err) => new RpcClientError.RpcClientError({ reason: err.reason }))),
      supportsAck: false,
      supportsTransferables: false,
      codecFor: RpcSerialization.json.codecFor,
    });
  });
  const receive = (raw: string) =>
    Effect.suspend(() => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        return Effect.succeed(false);
      }
      if (!isFrame(parsed)) return Effect.succeed(false);
      const m = (parsed as { m: unknown }).m;
      if (parsed.to === "server") return Effect.as(toServer(0, m as RpcMessage.FromClientEncoded), true);
      return Effect.as(
        Effect.forEach(clients, (id) => toClient(id, m as RpcMessage.FromServerEncoded), { discard: true }),
        true,
      );
    });
  const closed = (reason: Socket.SocketError["reason"]) =>
    Effect.forEach(clients, (id) => toClient(id, { _tag: "ClientProtocolError", error: new RpcClientError.RpcClientError({ reason }) }), { discard: true }).pipe(
      Effect.andThen(Queue.offer(disconnects, 0)),
    );
  return { server, client, receive, closed } satisfies Wire;
});

export const replay = (wire: Wire, tag: string, id: string | number) => wire.receive(JSON.stringify({ to: "server", m: { _tag: "Request", id, tag, payload: null, headers: [] } }));

export const closedBy = (code: number, closeReason?: string) => new Socket.SocketCloseError({ code, ...(closeReason === undefined ? {} : { closeReason }) });
export const writeFailed = (cause: unknown) => new Socket.SocketError({ reason: new Socket.SocketWriteError({ cause }) });
