export const MOVE_PROTOCOL = 5;

export const MOVE_PORT_NBD = 14652;
export const MOVE_PORT_IMAGE = 14653;
export const MOVE_PORT_STATUS = 14654;
export const MOVE_PORT_VERDICT = 14655;

export const MOVE_REGION_SECTORS = 1024;

export const STAGE_AT = 2 ** 20;
export const STAGE_MAGIC = "TSTAGE01";

export const MOVE_SLOTS = ["dummy", "root", "swap", "dummy", "dummy", "home", "net"] as const;
export type MoveSlot = (typeof MOVE_SLOTS)[number];
const BOX_SLOTS = MOVE_SLOTS.flatMap((role, i) => (role === "dummy" ? [] : [{ role, irq: 5 + i, mmio: 0xc0001000 + i * 0x1000 }]));
export const MOVE_BOX_CMDLINE = BOX_SLOTS.map((s) => `virtio_mmio.device=4K@0x${s.mmio.toString(16)}:${s.irq}`).join(" ");
export const moveGuestDevice = (role: "root" | "swap" | "home", pci = false): string =>
  pci ? `virtio-pci:0000:00:${(MOVE_SLOTS.indexOf(role) + 1).toString(16).padStart(2, "0")}.0` : `virtio:${BOX_SLOTS.findIndex((s) => s.role === role)}`;
