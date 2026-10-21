# IMsystem

基于 Go TCP 的简易即时通讯系统。

## 目录结构

```
.
├── cmd/
│   ├── server/main.go      # 服务端入口
│   └── client/main.go      # 客户端入口
├── internal/
│   └── imserver/
│       ├── server.go       # 服务器：用户管理、广播、命令分发
│       ├── user.go         # 用户：连接读写
│       └── command.go      # 命令解析
├── Makefile
└── go.mod
```

## 启动

需要开两个终端。

### 终端 1：启动服务端

```bash
# 方式一：go run
go run ./cmd/server

# 方式二：先编译再运行
make build
./bin/server.exe
```

启动后看到 `服务器监听 127.0.0.1:8888` 表示成功。

### 终端 2：启动客户端

```bash
go run ./cmd/client
```

看到 `连接服务器成功!` 表示连上了。

## 多终端验证聊天

开**两个或多个**客户端终端，每个都执行 `go run ./cmd/client`。

每个客户端默认名字是连接地址，先改名再聊天：

```
rename|张三
```

另一个终端：

```
rename|李四
```

### 验证广播

在张三的终端输入任意文字回车：

```
大家好
```

所有在线客户端（包括李四）都会收到：

```
[127.0.0.1:xxxxx]张三:大家好
```

### 验证私聊

张三给李四发私聊：

```
to|李四|在吗
```

只有李四的终端会收到：

```
张三对你说：在吗
```

### 查看在线用户

任意客户端输入：

```
who
```

会列出所有在线用户：

```
[127.0.0.1:xxxxx]张三:已上线
[127.0.0.1:yyyyy]李四:已上线
```

### 退出

输入：

```
exit
```

## 命令速查

| 命令 | 说明 |
|------|------|
| `who` | 查看在线用户 |
| `rename\|名字` | 修改自己的用户名 |
| `to\|用户名\|消息` | 私聊指定用户 |
| 任意文本 | 广播给所有在线用户 |
| `exit` | 退出客户端 |

## Makefile 命令

```bash
make build          # 编译服务端和客户端到 bin/
make run-server     # 运行服务端
make run-client     # 运行客户端
make vet            # go vet 检查
make fmt            # go fmt 格式化
make clean          # 清理构建产物
```

## 注意

- 服务端有 10 秒无操作超时断开机制，测试时持续输入即可。
- Windows 下 `make` 需在 Git Bash 中使用，PowerShell 请直接用 `go run ./cmd/server`。
