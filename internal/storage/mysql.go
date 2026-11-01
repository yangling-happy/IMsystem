package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"imsystem/internal/model"
)

// mysqlErrDuplicateEntry 是 MySQL 唯一索引冲突的错误码。
const mysqlErrDuplicateEntry = 1062

// MySQL 是 Repo 的真实实现。
type MySQL struct {
	db *sql.DB
}

// NewMySQL 建立连接池并做一次探活：配置错误要尽早暴露，而不是等第一条消息进来才失败。
func NewMySQL(dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	// 连接池参数：并发写入上限、空闲复用、单连接最长寿命（避免被数据库端判为空闲连接）
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &MySQL{db: db}, nil
}

// SaveMessage 依赖 uk_msg_id 唯一索引实现幂等：冲突即视为已落库。
func (m *MySQL) SaveMessage(ctx context.Context, msg model.Message) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO messages (msg_id, from_user, to_user, chat_type, body, status, created_at)
		 VALUES (?, ?, ?, ?, ?, 0, ?)`,
		msg.MsgID, msg.From, msg.To, string(msg.ChatType), msg.Body, msg.SentAt,
	)
	if err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == mysqlErrDuplicateEntry {
			return ErrDuplicate
		}
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

func (m *MySQL) Close() error {
	return m.db.Close()
}
