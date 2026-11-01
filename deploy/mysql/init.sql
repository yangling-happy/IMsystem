
CREATE TABLE IF NOT EXISTS messages (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    msg_id      VARCHAR(64)  NOT NULL,
    from_user   VARCHAR(64)  NOT NULL,
    to_user     VARCHAR(64)  NOT NULL,
    chat_type   VARCHAR(16)  NOT NULL DEFAULT 'single',
    body        TEXT         NOT NULL,
    status      TINYINT      NOT NULL DEFAULT 0 COMMENT '0 已持久化 1 已投递',
    created_at  DATETIME(3)  NOT NULL,
    delivered_at DATETIME(3) NULL,
    UNIQUE KEY uk_msg_id (msg_id),
    KEY idx_to_user (to_user, id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
