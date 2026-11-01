// Package storage 定义消息持久化接口，具体实现（MySQL）在后续阶段接入。
package storage

import (
	"context"
	"errors"

	"imsystem/internal/model"
)

// ErrDuplicate 表示 msg_id 已存在，调用方据此判定重复投递。
var ErrDuplicate = errors.New("storage: duplicate message")

// Repo 是消息持久化抽象，便于单测替换和后续换库。
type Repo interface {
	// SaveMessage 落库一条消息；MsgID 冲突时返回 ErrDuplicate。
	SaveMessage(ctx context.Context, m model.Message) error
	Close() error
}
