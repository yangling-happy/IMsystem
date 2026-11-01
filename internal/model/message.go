// Package model 存放跨层共享的数据结构，不依赖任何存储或网络实现。
package model

import "time"

// ChatType 消息类型。
type ChatType string

const (
	ChatSingle ChatType = "single"
	ChatGroup  ChatType = "group"
)

// Message 是一条待投递/已投递的聊天消息，MsgID 由发送方生成，用于幂等去重。
type Message struct {
	MsgID    string   `json:"msg_id"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	ChatType ChatType `json:"chat_type"`
	Body     string   `json:"body"`
	SentAt   time.Time `json:"sent_at"`
}
