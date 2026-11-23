export type OrganizationId = string;

export const ORG_ID_PATTERN = /^org_[0-9a-f]{12}$/;
export const BOX_ID_PATTERN = /^box_[0-9a-f]{16}$/;
export const THREAD_ID_PATTERN = /^thr_[0-9a-f]{12}$/;

export interface Org {
  id: OrganizationId;
  name: string;
  createdAt: string;
}

export type OrgRole = "admin" | "member";

export type MembershipStatus = "active" | "removed";

export type MembershipKind = "human" | "service";

export interface CurrentMembership {
  id: string;
  role: OrgRole;
}

export function memberLabel(m: { email: string | null; name: string | null }): string {
  if (m.email !== null) return m.email;
  if (m.name !== null) return `service:${m.name}`;
  return "?";
}
