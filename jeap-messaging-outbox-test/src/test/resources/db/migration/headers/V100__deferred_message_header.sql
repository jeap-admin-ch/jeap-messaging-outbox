CREATE TABLE deferred_message_header
(
    deferred_message_id BIGINT NOT NULL,
    header_index INTEGER NOT NULL,
    header_name VARCHAR(255) NOT NULL,
    header_value VARBINARY,
    PRIMARY KEY (deferred_message_id, header_index),
    FOREIGN KEY (deferred_message_id) REFERENCES deferred_message (id) ON DELETE CASCADE
);
