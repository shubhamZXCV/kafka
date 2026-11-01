#include "elog/coordinator/schema.h"

#include <string>

#include "elog/coordinator/db.h"

namespace elog {

namespace {

// LLD §8.2, verbatim.
constexpr const char* kSchemaSql = R"sql(
CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version VALUES (1);

CREATE TABLE broker (
    broker_id    INTEGER PRIMARY KEY,
    host         TEXT    NOT NULL,
    port         INTEGER NOT NULL,
    incarnation  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE topic (
    name                TEXT PRIMARY KEY,
    partitions          INTEGER NOT NULL CHECK (partitions > 0),
    replication_factor  INTEGER NOT NULL CHECK (replication_factor > 0),
    min_isr             INTEGER NOT NULL CHECK (min_isr > 0 AND min_isr <= replication_factor)
);

CREATE TABLE partition_state (
    topic         TEXT    NOT NULL REFERENCES topic(name),
    partition     INTEGER NOT NULL,
    leader        INTEGER,                         -- NULL = offline
    leader_epoch  INTEGER NOT NULL DEFAULT 0,
    isr           TEXT    NOT NULL,                -- JSON array, e.g. "[1,2,3]"
    PRIMARY KEY (topic, partition)
);

CREATE TABLE partition_replica (
    topic      TEXT    NOT NULL,
    partition  INTEGER NOT NULL,
    broker_id  INTEGER NOT NULL REFERENCES broker(broker_id),
    ordinal    INTEGER NOT NULL,                   -- preference order
    PRIMARY KEY (topic, partition, broker_id),
    FOREIGN KEY (topic, partition) REFERENCES partition_state(topic, partition)
);

CREATE TABLE consumer_group (
    group_id    TEXT PRIMARY KEY,
    generation  INTEGER NOT NULL DEFAULT 0,
    assignor    TEXT    NOT NULL CHECK (assignor IN ('roundrobin', 'sticky'))
);

CREATE TABLE partition_ownership (
    group_id      TEXT    NOT NULL REFERENCES consumer_group(group_id),
    topic         TEXT    NOT NULL,
    partition     INTEGER NOT NULL,
    owner_member  TEXT    NOT NULL,
    owner_epoch   INTEGER NOT NULL,
    PRIMARY KEY (group_id, topic, partition)
);

CREATE TABLE committed_offset (
    group_id         TEXT    NOT NULL REFERENCES consumer_group(group_id),
    topic            TEXT    NOT NULL,
    partition        INTEGER NOT NULL,
    offset           INTEGER NOT NULL CHECK (offset >= 0),
    owner_epoch      INTEGER NOT NULL,
    committed_at_ms  INTEGER NOT NULL,
    PRIMARY KEY (group_id, topic, partition)
);
)sql";

}  // namespace

Status ensureSchema(Db& db) {
    auto exists = db.prepare(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'");
    if (!exists.ok()) {
        return {exists.error(), exists.message()};
    }
    if (auto row = exists.value()->step(); !row.ok()) {
        return {row.error(), row.message()};
    }
    const bool fresh = exists.value()->columnInt(0) == 0;
    exists.value()->reset();
    if (fresh) {
        auto txn = db.begin();
        if (!txn.ok()) {
            return {txn.error(), txn.message()};
        }
        if (auto status = db.exec(kSchemaSql); !status.ok()) {
            return status;  // txn rolls back
        }
        return txn.value().commit();
    }

    auto version = db.prepare("SELECT version FROM schema_version");
    if (!version.ok()) {
        return {version.error(), version.message()};
    }
    auto row = version.value()->step();
    if (!row.ok()) {
        return {row.error(), row.message()};
    }
    const bool current = row.value() && version.value()->columnInt(0) == kSchemaVersion;
    version.value()->reset();
    if (!current) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR,
                "database schema version is not " + std::to_string(kSchemaVersion)};
    }
    return okStatus();
}

}  // namespace elog
