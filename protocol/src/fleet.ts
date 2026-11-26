import { Schema } from "effect";

export const VM_ID_PREFIX = "vm-";

export const FLEET_HELLO_MS = 15_000;
export const FLEET_LEASE_MS = 60_000;
export const FLEET_USAGE_MS = 10_000;
export const MAX_FLEET_FILES = 3;
export const HOSTD_DATADOG_PARAMETER = "/tesser-hostd/dd-api-key";

export const FLEET_HOST_TAG = "tesser:fleet-host";
export const HOST_ID = /^h-[0-9a-f]{12}$/;
export const DEVICE_HOST_TYPE = "device";
export const DEVICE_NAME = /^[a-z0-9][a-z0-9-]{0,31}$/;
export const MAX_DEVICES = 5;
export const RELAY_SESSION = /^[0-9a-f]{64}$/;
export const RELAY_PAIRED = "paired";
export const HOST_SIGNING_KEY = /^[A-Za-z0-9_-]{43}$/;
export const SSH_PORTS = { from: 20_000, to: 21_999 };
export const EXPORT_PORT = 22_000;
export const MUX_PORT = 22_001;
export const WG_PORT = 51820;
export const FLEET_GATEWAY = "10.128.0.1";
export const BASE_CACHE_PORT = 14660;
export const WG_KEY = /^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$/;
export const WG_DOOR = /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}:2[01]\d{3}$/;
export const MAX_HOST_INDEX = 2047;
export const MAX_SLOT = 2000;

export function hostAddress(index: number): string {
  return `10.127.${index >> 8}.${index & 255}`;
}

export const HOST_TOKEN_TTL_S = 60;

export const FLEET_HOST_TYPES: Record<string, { vcpus: number; memoryMiB: number; awsCentsPerHour: number }> = {
  "m8azn.metal-12xl": { vcpus: 48, memoryMiB: 192 * 1024, awsCentsPerHour: 495.504 },
  "m8a.metal-24xl": { vcpus: 96, memoryMiB: 384 * 1024, awsCentsPerHour: 584.256 },
  "r8a.metal-24xl": { vcpus: 96, memoryMiB: 768 * 1024, awsCentsPerHour: 766.848 },
  "r8a.metal-48xl": { vcpus: 192, memoryMiB: 1536 * 1024, awsCentsPerHour: 1533.696 },
};

export const HOST_MIN_FREE_BYTES = 30 * 2 ** 30;

export function vmQuotaMiB(rootMiB: number, vm: { memoryMiB: number; dataDiskGb: number | null }): number {
  return rootMiB + (vm.dataDiskGb ?? 0) * 1024 + 2 * vm.memoryMiB + 1024;
}

export function isVmId(instanceId: string): boolean {
  return instanceId.startsWith(VM_ID_PREFIX);
}

export function isDeviceFleet(fleetId: string | null): fleetId is string {
  return fleetId !== null && HOST_ID.test(fleetId);
}

export const HostdBuild = Schema.Struct({ version: Schema.String.check(Schema.isPattern(/^hostd-[0-9a-f]{12}$/)), sha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)) });
export type HostdBuild = typeof HostdBuild.Type;
