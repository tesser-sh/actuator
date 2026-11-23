export const ENV_KEY_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export const AWS_REGION_RE = /^[a-z]{2}(-[a-z]+)+-\d{1,2}$/;

const DENIED_KEYS = new Set([
  "PATH",
  "HOME",
  "SHELL",
  "ENV",
  "IFS",
  "CDPATH",
  "SHELLOPTS",
  "BASHOPTS",
  "PROMPT_COMMAND",
  "PS0",
  "PS1",
  "PS2",
  "PS3",
  "PS4",
  "ZDOTDIR",
  "INPUTRC",
  "GCONV_PATH",
  "GLIBC_TUNABLES",
  "EDITOR",
  "VISUAL",
  "PAGER",
  "MANPAGER",
  "LESSOPEN",
  "LESSCLOSE",
  "BROWSER",
  "NODE_OPTIONS",
  "NODE_PATH",
  "NODE_REPL_EXTERNAL_MODULE",
  "NODE_EXTRA_CA_CERTS",
  "NODE_TLS_REJECT_UNAUTHORIZED",
  "RUBYOPT",
  "RUBYLIB",
  "GEM_HOME",
  "GEM_PATH",
  "JAVA_TOOL_OPTIONS",
  "_JAVA_OPTIONS",
  "JDK_JAVA_OPTIONS",
  "JAVA_OPTS",
  "MAVEN_OPTS",
  "GRADLE_OPTS",
  "CLASSPATH",
  "CC",
  "CXX",
  "CPP",
  "LD",
  "RUSTC",
  "RUSTDOC",
  "RUSTC_WRAPPER",
  "RUSTC_WORKSPACE_WRAPPER",
  "RUSTFLAGS",
  "GOFLAGS",
  "GOPROXY",
  "GOSUMDB",
  "GONOSUMDB",
  "GONOSUMCHECK",
  "GOINSECURE",
  "GOTOOLCHAIN",
  "GOENV",
  "UV_INDEX",
  "UV_INDEX_URL",
  "UV_DEFAULT_INDEX",
  "UV_EXTRA_INDEX_URL",
  "UV_FIND_LINKS",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "ALL_PROXY",
  "SSL_CERT_FILE",
  "SSL_CERT_DIR",
  "CURL_CA_BUNDLE",
  "REQUESTS_CA_BUNDLE",
]);

const DENIED_PREFIXES = ["LD_", "DYLD_", "BASH_", "GIT_", "SSH_", "NPM_CONFIG_", "PNPM_", "YARN_", "BUN_", "COREPACK_", "PYTHON", "PERL", "PIP_", "BUNDLE_", "CARGO_"];

const HARMLESS_KEYS = new Set(["PYTHONUNBUFFERED", "PYTHONDONTWRITEBYTECODE", "PYTHONIOENCODING", "PYTHONUTF8", "PYTHONHASHSEED"]);

export function envKeyRefusal(key: string): string | null {
  if (!ENV_KEY_RE.test(key)) return `"${key}" is not an env var name`;
  const upper = key.toUpperCase();
  if (HARMLESS_KEYS.has(upper)) return null;
  if (DENIED_KEYS.has(upper)) return `${key} can run code on the box, so the control plane may not set it`;
  const prefix = DENIED_PREFIXES.find((p) => upper.startsWith(p));
  if (prefix !== undefined) return `${key} can run code on the box (${prefix}* is refused), so the control plane may not set it`;
  return null;
}

export function awsRegionRefusal(region: string): string | null {
  return AWS_REGION_RE.test(region) ? null : `"${region}" is not an AWS region`;
}

const DOTENV_LINE_RE = /^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/;
const DOUBLE_RE = /^"((?:[^"\\]|\\.)*)"\s*(?:#.*)?$/s;
const SINGLE_RE = /^'([^']*)'\s*(?:#.*)?$/s;
const ESCAPES: Record<string, string> = { n: "\n", r: "\r", t: "\t", '"': '"', "\\": "\\" };

export function parseDotenv(text: string, onBadLine: (line: number) => void = () => {}): Record<string, string> {
  const lines = text.split("\n").map((line) => line.replace(/\r$/, ""));
  const env: Record<string, string> = {};
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*(#|$)/.test(lines[i]!)) continue;
    const m = DOTENV_LINE_RE.exec(lines[i]!);
    if (m === null) {
      onBadLine(i + 1);
      continue;
    }
    const first = m[2]!;
    const quoted = first.startsWith('"') ? DOUBLE_RE : first.startsWith("'") ? SINGLE_RE : null;
    let value = first.replace(/\s#.*$/, "").trimEnd();
    if (quoted !== null) {
      let rest = first;
      let end = i;
      while (!quoted.test(rest) && end + 1 < lines.length) rest += `\n${lines[++end]}`;
      const q = quoted.exec(rest);
      if (q !== null) {
        value = quoted === DOUBLE_RE ? q[1]!.replaceAll(/\\(.)/gs, (all, c: string) => ESCAPES[c] ?? all) : q[1]!;
        i = end;
      }
    }
    if (!value.includes("\0")) env[m[1]!] = value;
  }
  return env;
}
