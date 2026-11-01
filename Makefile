# Go IM System Makefile

GO      := go
SERVER  := ./cmd/server
CLIENT  := ./cmd/client
BIN_DIR := bin
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: all build build-server build-client run-server run-client clean vet fmt tidy test up down

all: build

# 编译服务端和客户端到 bin/ 目录
build: build-server build-client

build-server:
	$(GO) build -o $(BIN_DIR)/server.exe $(SERVER)

build-client:
	$(GO) build -o $(BIN_DIR)/client.exe $(CLIENT)

# 直接运行
run-server:
	$(GO) run $(SERVER)

run-client:
	$(GO) run $(CLIENT)

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