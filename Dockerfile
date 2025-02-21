FROM rust:1.83-alpine3.21 AS chef
WORKDIR /usr/src/platoon
RUN apk add --no-cache musl-dev protobuf protobuf-dev && cargo install cargo-chef

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /usr/src/platoon/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin platoon

FROM alpine:3.21
COPY --from=builder /usr/src/platoon/target/release/platoon /usr/local/bin/platoon
CMD ["platoon"]
