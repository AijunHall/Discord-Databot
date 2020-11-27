CREATE TABLE messages (
    message_id BIGINT,
    user_id BIGINT,
    server_id BIGINT,
    channel_id BIGINT,
    message_datetime DATETIME(3),
    message_content VARCHAR(2001),
    PRIMARY KEY(message_id, user_id, server_id, channel_id)
);

CREATE TABLE attachments (
    message_id BIGINT,
    user_id BIGINT,
    server_id BIGINT,
    channel_id BIGINT,
    attachment_datetime DATETIME(3),
    attachment_content VARCHAR(2001),
    PRIMARY KEY(message_id, user_id, server_id, channel_id)
);

CREATE TABLE servers (
    server_id BIGINT PRIMARY KEY,
    channel_count BIGINT,
    user_count BIGINT,
    message_count BIGINT,
    attachment_count BIGINT
);

CREATE TABLE channels (
    channel_id BIGINT PRIMARY KEY,
    server_id BIGINT,
    message_count BIGINT,
    attachment_count BIGINT
);

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    server_count BIGINT,
    message_count BIGINT,
    attachment_count BIGINT
);
