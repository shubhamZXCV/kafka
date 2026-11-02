#include "elog/coordinator/repository.h"

#include <exception>
#include <utility>

#include <nlohmann/json.hpp>

namespace elog {

namespace {

// partition_state.isr is a JSON array, e.g. "[1,2,3]" (LLD §8.2).
std::string toJsonArray(const std::vector<BrokerId>& ids) {
    return nlohmann::json(ids).dump();
}

std::vector<BrokerId> fromJsonArray(const std::string& text) {
    return nlohmann::json::parse(text).get<std::vector<BrokerId>>();
}

// Calls fn(stmt) for each result row; returns the first step error.
template <class Fn>
Status forEachRow(Stmt& stmt, Fn fn) {
    while (true) {
        auto row = stmt.step();
        if (!row.ok()) {
            return {row.error(), row.message()};
        }
        if (!row.value()) {
            return okStatus();
        }
        fn(stmt);
    }
}

Status stepToEnd(Stmt& stmt) {
    auto row = stmt.step();
    if (!row.ok()) {
        return {row.error(), row.message()};
    }
    if (row.value()) {
        stmt.reset();
    }
    return okStatus();
}

}  // namespace

Result<std::unique_ptr<Repository>> Repository::open(const std::string& path) {
    auto db = Db::open(path);
    if (!db.ok()) {
        return {db.error(), db.message()};
    }
    return std::unique_ptr<Repository>(new Repository(std::move(db).value()));
}

Status Repository::createTopic(const TopicRecord& topic) {
    auto txn = db_->begin();
    if (!txn.ok()) {
        return {txn.error(), txn.message()};
    }
    auto insertTopic = db_->prepare(
        "INSERT INTO topic (name, partitions, replication_factor, min_isr) "
        "VALUES (:name, :partitions, :rf, :min_isr)");
    if (!insertTopic.ok()) {
        return {insertTopic.error(), insertTopic.message()};
    }
    Stmt& t = *insertTopic.value();
    t.bind(":name", topic.name);
    t.bind(":partitions", std::int64_t{topic.partitions});
    t.bind(":rf", std::int64_t{topic.replicationFactor});
    t.bind(":min_isr", std::int64_t{topic.minIsr});
    if (auto status = stepToEnd(t); !status.ok()) {
        return status;  // txn rolls back
    }
    for (const PartitionRecord& p : topic.partitionStates) {
        auto state = db_->prepare(
            "INSERT INTO partition_state (topic, partition, leader, leader_epoch, isr) "
            "VALUES (:t, :p, :leader, :epoch, :isr)");
        if (!state.ok()) {
            return {state.error(), state.message()};
        }
        Stmt& s = *state.value();
        s.bind(":t", topic.name);
        s.bind(":p", std::int64_t{p.partition});
        p.leader == kNoLeader ? s.bindNull(":leader") : s.bind(":leader", std::int64_t{p.leader});
        s.bind(":epoch", std::int64_t{p.leaderEpoch});
        s.bind(":isr", toJsonArray(p.isr));
        if (auto status = stepToEnd(s); !status.ok()) {
            return status;
        }
        for (std::size_t ordinal = 0; ordinal < p.replicas.size(); ++ordinal) {
            auto replica = db_->prepare(
                "INSERT INTO partition_replica (topic, partition, broker_id, ordinal) "
                "VALUES (:t, :p, :b, :ordinal)");
            if (!replica.ok()) {
                return {replica.error(), replica.message()};
            }
            Stmt& r = *replica.value();
            r.bind(":t", topic.name);
            r.bind(":p", std::int64_t{p.partition});
            r.bind(":b", std::int64_t{p.replicas[ordinal]});
            r.bind(":ordinal", static_cast<std::int64_t>(ordinal));
            if (auto status = stepToEnd(r); !status.ok()) {
                return status;
            }
        }
    }
    return txn.value().commit();
}

Result<std::uint32_t> Repository::registerBroker(BrokerId id, const std::string& host,
                                                 std::uint16_t port) {
    auto stmt = db_->prepare(
        "INSERT INTO broker (broker_id, host, port, incarnation) VALUES (:id, :h, :port, 1) "
        "ON CONFLICT (broker_id) DO UPDATE SET host = excluded.host, port = excluded.port, "
        "incarnation = broker.incarnation + 1 RETURNING incarnation");
    if (!stmt.ok()) {
        return {stmt.error(), stmt.message()};
    }
    Stmt& s = *stmt.value();
    s.bind(":id", std::int64_t{id});
    s.bind(":h", host);
    s.bind(":port", std::int64_t{port});
    auto row = s.step();
    if (!row.ok()) {
        return {row.error(), row.message()};
    }
    const auto incarnation = static_cast<std::uint32_t>(s.columnInt(0));
    if (auto status = stepToEnd(s); !status.ok()) {  // finishes the statement: commits the row
        return {status.error(), status.message()};
    }
    return incarnation;
}

Result<std::vector<TopicRecord>> Repository::loadTopics() {
    auto stmt = db_->prepare(
        "SELECT name, partitions, replication_factor, min_isr FROM topic ORDER BY name");
    if (!stmt.ok()) {
        return {stmt.error(), stmt.message()};
    }
    std::vector<TopicRecord> topics;
    auto status = forEachRow(*stmt.value(), [&](Stmt& s) {
        topics.push_back({s.columnText(0),
                          static_cast<std::uint32_t>(s.columnInt(1)),
                          static_cast<std::uint32_t>(s.columnInt(2)),
                          static_cast<std::uint32_t>(s.columnInt(3)),
                          {}});
    });
    if (!status.ok()) {
        return {status.error(), status.message()};
    }
    for (TopicRecord& topic : topics) {
        try {
            status = loadPartitions(topic);
        } catch (const std::exception& e) {  // a partition out of range, or a bad isr array
            status = {ErrorCode::UNKNOWN_SERVER_ERROR,
                      "corrupt topic " + topic.name + ": " + e.what()};
        }
        if (!status.ok()) {
            return {status.error(), status.message()};
        }
    }
    return topics;
}

Status Repository::loadPartitions(TopicRecord& topic) {
    topic.partitionStates.resize(topic.partitions);
    auto states = db_->prepare(
        "SELECT partition, leader, leader_epoch, isr FROM partition_state WHERE topic = :t");
    if (!states.ok()) {
        return {states.error(), states.message()};
    }
    states.value()->bind(":t", topic.name);
    auto status = forEachRow(*states.value(), [&](Stmt& s) {
        PartitionRecord& p = topic.partitionStates.at(static_cast<std::size_t>(s.columnInt(0)));
        p.partition = static_cast<std::uint32_t>(s.columnInt(0));
        p.leader = s.columnIsNull(1) ? kNoLeader : static_cast<BrokerId>(s.columnInt(1));
        p.leaderEpoch = static_cast<Epoch>(s.columnInt(2));
        p.isr = fromJsonArray(s.columnText(3));
    });
    if (!status.ok()) {
        return status;
    }
    auto replicas = db_->prepare(
        "SELECT partition, broker_id FROM partition_replica WHERE topic = :t "
        "ORDER BY partition, ordinal");
    if (!replicas.ok()) {
        return {replicas.error(), replicas.message()};
    }
    replicas.value()->bind(":t", topic.name);
    return forEachRow(*replicas.value(), [&](Stmt& r) {
        topic.partitionStates.at(static_cast<std::size_t>(r.columnInt(0)))
            .replicas.push_back(static_cast<BrokerId>(r.columnInt(1)));
    });
}

Result<Txn> Repository::begin() {
    return db_->begin();
}

Status Repository::createGroup(const std::string& groupId, const std::string& assignor) {
    auto stmt = db_->prepare(
        "INSERT INTO consumer_group (group_id, generation, assignor) VALUES (:g, 0, :a) "
        "ON CONFLICT (group_id) DO NOTHING");
    if (!stmt.ok()) {
        return {stmt.error(), stmt.message()};
    }
    stmt.value()->bind(":g", groupId);
    stmt.value()->bind(":a", assignor);
    return stepToEnd(*stmt.value());
}

Status Repository::saveGeneration(const std::string& groupId, Generation generation,
                                  const std::vector<OwnershipChange>& diff) {
    auto txn = db_->begin();
    if (!txn.ok()) {
        return {txn.error(), txn.message()};
    }
    auto gen = db_->prepare("UPDATE consumer_group SET generation = :gen WHERE group_id = :g");
    if (!gen.ok()) {
        return {gen.error(), gen.message()};
    }
    gen.value()->bind(":gen", std::int64_t{generation});
    gen.value()->bind(":g", groupId);
    if (auto status = stepToEnd(*gen.value()); !status.ok()) {
        return status;  // txn rolls back
    }
    for (const OwnershipChange& change : diff) {
        auto stmt = db_->prepare(
            change.owner
                ? "INSERT INTO partition_ownership (group_id, topic, partition, owner_member, "
                  "owner_epoch) VALUES (:g, :t, :p, :owner, :epoch) "
                  "ON CONFLICT (group_id, topic, partition) DO UPDATE SET "
                  "owner_member = excluded.owner_member, owner_epoch = excluded.owner_epoch"
                : "DELETE FROM partition_ownership "
                  "WHERE group_id = :g AND topic = :t AND partition = :p");
        if (!stmt.ok()) {
            return {stmt.error(), stmt.message()};
        }
        Stmt& s = *stmt.value();
        s.bind(":g", groupId);
        s.bind(":t", change.tp.topic);
        s.bind(":p", std::int64_t{change.tp.partition});
        if (change.owner) {
            s.bind(":owner", *change.owner);
            s.bind(":epoch", std::int64_t{change.epoch});
        }
        if (auto status = stepToEnd(s); !status.ok()) {
            return status;
        }
    }
    return txn.value().commit();
}

Status Repository::commitOffsetUnfenced(const std::string& groupId, const TopicPartition& tp,
                                        Offset offset, Epoch epoch, TimeMs nowMs) {
    // LLD §8.3, unfenced variant: no ownership check at all.
    auto stmt = db_->prepare(
        "INSERT INTO committed_offset (group_id, topic, partition, offset, owner_epoch, "
        "committed_at_ms) VALUES (:g, :t, :p, :offset, :epoch, :now) "
        "ON CONFLICT (group_id, topic, partition) DO UPDATE SET offset = excluded.offset, "
        "owner_epoch = excluded.owner_epoch, committed_at_ms = excluded.committed_at_ms");
    if (!stmt.ok()) {
        return {stmt.error(), stmt.message()};
    }
    Stmt& s = *stmt.value();
    s.bind(":g", groupId);
    s.bind(":t", tp.topic);
    s.bind(":p", std::int64_t{tp.partition});
    s.bind(":offset", static_cast<std::int64_t>(offset));
    s.bind(":epoch", std::int64_t{epoch});
    s.bind(":now", nowMs);
    return stepToEnd(s);
}

Result<std::optional<Offset>> Repository::committedOffset(const std::string& groupId,
                                                          const TopicPartition& tp) {
    auto stmt = db_->prepare(
        "SELECT offset FROM committed_offset WHERE group_id = :g AND topic = :t AND partition = "
        ":p");
    if (!stmt.ok()) {
        return {stmt.error(), stmt.message()};
    }
    Stmt& s = *stmt.value();
    s.bind(":g", groupId);
    s.bind(":t", tp.topic);
    s.bind(":p", std::int64_t{tp.partition});
    auto row = s.step();
    if (!row.ok()) {
        return {row.error(), row.message()};
    }
    if (!row.value()) {
        return std::optional<Offset>{};
    }
    const auto offset = static_cast<Offset>(s.columnInt(0));
    s.reset();
    return std::optional<Offset>{offset};
}

}  // namespace elog
