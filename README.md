# tesser actuator

The actuator is the only thing that calls AWS in a self-hosted tesser account. It runs in your account as a role you create, dials out to tesser over one websocket, and serves the fixed list of operations in `OPS` (`protocol/src/actuator-rpc.ts`). Before every call it checks the request against your config: the org, region, subnets, security group, image owners, tags and rate limits you set. It prints an audit line for every call to stdout, which your task's log group keeps. Tesser holds no standing credentials into your account.

This repository is a read-only mirror. Each tesser release exports the actuator's exact source from tesser's private monorepo as one commit, `vX.Y.Z (tesser-sh/tesser@<commit>)`, tagged `vX.Y.Z`. Pull requests are not accepted.

## What is here

- `actuator/src`: the actuator. `main.ts` starts it, `link.ts` is the websocket to tesser, `guard.ts` checks every call, `ops.ts` makes the AWS calls, `audit.ts` writes the audit log.
- `protocol/src`: the parts of tesser's wire protocol the actuator imports, and nothing else. `actuator-rpc.ts` is every operation and its schema, `actuator-fence.ts` is the per-operation checks.
- `actuator/Dockerfile`: the image, a `bun build --compile` binary on distroless.
- `.github/workflows/release.yml`: on each tag, typechecks, builds the image for ARM64, pushes it to `public.ecr.aws/tesser/actuator:<version>`, and attests its build provenance.

## Verify an image

Find the digest of a version:

```sh
docker buildx imagetools inspect public.ecr.aws/tesser/actuator:<version>
```

Check that this repository's release workflow built that digest:

```sh
gh attestation verify oci://public.ecr.aws/tesser/actuator@sha256:<digest> -R tesser-sh/actuator
```

The attestation names the commit and tag it was built from, so the image is the source at that tag. Pin the digest, not the tag, in your config.
