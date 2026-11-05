# Go IM System Makefile

GO      := go
SERVER  := ./cmd/server
CLIENT  := ./cmd/client
BENCH   := ./cmd/bench
BIN_DIR := bin
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: all build build-server build-client build-bench run-server run-client run-bench clean vet fmt tidy test up down

all: build

# 编译服务端、客户端和压测工具到 bin/ 目录
build: build-server build-client build-bench

build-server:
	$(GO) build -o $(BIN_DIR)/server.exe $(SERVER)

build-client:
	$(GO) build -o $(BIN_DIR)/client.exe $(CLIENT)

build-bench:
	$(GO) build -o $(BIN_DIR)/bench.exe $(BENCH)

# 直接运行
run-server:
	$(GO) run $(SERVER)

run-client:
	$(GO) run $(CLIENT)

# 压测：20 条连接 × 每条 20 条消息（需先启动服务端）
run-bench:
	$(GO) run $(BENCH) -conns 20 -msgs 20

# 代码质量
vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

# 清理构建产物
clean:
	rm -rf $(BIN_DIR) chatserver chatserver.exe

test:
	$(GO) test ./...

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down