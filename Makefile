.PHONY: build build-publisher build-consumer build-monitor run-publisher run-consumer run-monitor start-nats stop-nats clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  build           - Build publisher, consumer, and monitor"
	@echo "  build-publisher - Build publisher only"
	@echo "  build-consumer  - Build consumer only"
	@echo "  build-monitor   - Build monitor only"
	@echo "  run-publisher   - Run publisher"
	@echo "  run-consumer    - Run consumer"
	@echo "  run-monitor     - Run monitor (process management UI)"
	@echo "  start-nats      - Start NATS with monitoring UI"
	@echo "  stop-nats       - Stop NATS"
	@echo "  clean           - Clean build artifacts"

# Build targets
build: build-publisher build-consumer build-monitor

build-publisher:
	go build -o bin/publisher cmd/publisher/main.go

build-consumer:
	go build -o bin/consumer cmd/consumer/main.go

build-monitor:
	go build -o bin/monitor cmd/monitor/monitor.go

# Run targets
run-publisher: build-publisher
	./bin/publisher

run-consumer: build-consumer
	./bin/consumer

run-monitor: build-monitor
	./bin/monitor

# NATS targets
start-nats:
	docker-compose -f docker-compose-with-web-ui.yml up -d

stop-nats:
	docker-compose -f docker-compose-with-web-ui.yml down

# Clean target
clean:
	rm -f bin/publisher bin/consumer bin/monitor