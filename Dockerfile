FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the applications
RUN go build -o bin/monitor cmd/monitor/monitor.go
RUN go build -o bin/consumer cmd/consumer/main.go
RUN go build -o bin/publisher cmd/publisher/main.go

# Final stage
FROM alpine:latest

WORKDIR /root/

# Install curl for testing
RUN apk add --no-cache curl

# Copy the binaries from builder stage
COPY --from=builder /app/bin/monitor .
COPY --from=builder /app/bin/consumer .
COPY --from=builder /app/bin/publisher .

# Expose port
EXPOSE 8082

# Run the binary
CMD ["./monitor"]