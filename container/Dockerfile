# Multi-stage build for smaller image
FROM rust:alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

WORKDIR /build

# Copy the source code
COPY Cargo.toml ./
COPY src ./src

# Build the release binary
RUN cargo build --release

# Final stage - minimal alpine image
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/cache/apk/*

WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /build/target/release/video-transcoder /app/transcoder

# Make it executable
RUN chmod +x /app/transcoder

# Set the entrypoint
ENTRYPOINT ["/app/transcoder"]
