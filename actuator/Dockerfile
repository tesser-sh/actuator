FROM --platform=$BUILDPLATFORM oven/bun:1.4.2 AS build
ARG VERSION=0.0.0
WORKDIR /src
COPY . .
RUN bun install --frozen-lockfile --ignore-scripts
RUN bun build --compile --minify --target=bun-linux-arm64 --define "__ACTUATOR_VERSION__=\"${VERSION}\"" actuator/src/main.ts --outfile /actuator

FROM --platform=linux/arm64 gcr.io/distroless/base-debian12:nonroot
COPY --from=build /actuator /actuator
ENTRYPOINT ["/actuator"]
