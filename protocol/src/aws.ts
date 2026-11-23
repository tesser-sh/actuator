export const VCPU_QUOTA_CODE = "L-1216C47A";

const MAX_CLOUD_TAGS = 30;

const TAG_CHARS = /^[\p{L}\p{N}\p{Zs}_.:/=+\-@]*$/u;

function cloudTagProblem(key: string, value: string): string | null {
  if (key.length < 1 || key.length > 128) return `key "${key}" must be 1 to 128 characters`;
  if (value.length > 256) return `${key}'s value must be at most 256 characters`;
  if (/^tesser:/i.test(key)) return `key "${key}" is reserved for tesser`;
  if (/^aws:/i.test(key)) return `key "${key}" is reserved for AWS`;
  if (key === "Name") return `key "Name" is reserved: tesser names what it creates`;
  if (!TAG_CHARS.test(key) || !TAG_CHARS.test(value)) return `"${key}=${value}" has a character AWS tags reject; use letters, digits, spaces, and _ . : / = + - @`;
  return null;
}

export function cloudTagsProblem(tags: Record<string, string>): string | null {
  const entries = Object.entries(tags);
  if (entries.length > MAX_CLOUD_TAGS) return `at most ${MAX_CLOUD_TAGS} tags: tesser's own need the rest of AWS's 50 per resource`;
  for (const [key, value] of entries) {
    const problem = cloudTagProblem(key, value);
    if (problem !== null) return problem;
  }
  return null;
}
