// Package protocol 定义客户端与服务端之间的报文格式：
// 一行一个 JSON 对象，以 '\n' 结尾（JSON Lines）。
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
)

// 报文类型。目前只区分"谁在说话"，不做多余扩展。
const (
	TypeChat      = "chat"      // 聊天消息
	TypeWho       = "who"       // 查询在线用户
	TypeRename    = "rename"    // 改名
	TypeHeartbeat = "heartbeat" // 心跳，仅用于维持连接活跃
	TypeNotice    = "notice"    // 服务端下发的系统提示
	TypeAck       = "ack"       // 服务端确认"消息已受理"，客户端据此停止重发
)

// Envelope 是传输层的一条报文。
// 为什么不用 string 拼 "to|名字|内容"：因为 msg_id、chat_type 这类字段
// 一旦出现，字符串协议就要靠转义规则硬编码，永远长不大。
type Envelope struct {
	Type     string `json:"type"`
	MsgID    string `json:"msg_id,omitempty"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	ChatType string `json:"chat_type,omitempty"`
	Body     string `json:"body,omitempty"`
}

// ErrEmptyLine 表示收到一个空行（读循环里直接跳过，不算错误）。
var ErrEmptyLine = errors.New("protocol: empty line")

// Encode 把报文编码成一行，返回值以 '\n' 结尾。
func Encode(e Envelope) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Decode 把一行原始字节解析成报文。容忍行尾的 \r\n。
func Decode(line []byte) (Envelope, error) {
	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return Envelope{}, ErrEmptyLine
	}
	var e Envelope
	if err := json.Unmarshal(line, &e); err != nil {
		return Envelope{}, err
	}
	if e.Type == "" {
		var ErrMissingType = errors.New("protocol: missing type")
		return Envelope{}, ErrMissingType
	}
	return e, nil
}
