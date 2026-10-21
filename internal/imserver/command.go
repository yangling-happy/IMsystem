package imserver

import "strings"

// CommandType 消息命令类型
type CommandType int

const (
	CmdBroadcast CommandType = iota
	CmdWho
	CmdRename
	CmdTo
)

// Command 解析后的命令
type Command struct {
	Type    CommandType
	Target  string
	Content string
}

// ParseCommand 把原始字符串解析成命令，纯函数无副作用
func ParseCommand(raw string) Command {
	raw = strings.TrimRight(raw, "\r\n")

	if raw == "who" {
		return Command{Type: CmdWho}
	}

	if strings.HasPrefix(raw, "rename|") {
		return Command{Type: CmdRename, Content: strings.TrimPrefix(raw, "rename|")}
	}

	if strings.HasPrefix(raw, "to|") {
		parts := strings.SplitN(raw, "|", 3)
		if len(parts) == 3 {
			return Command{Type: CmdTo, Target: parts[1], Content: parts[2]}
		}
	}

	return Command{Type: CmdBroadcast, Content: raw}
}
