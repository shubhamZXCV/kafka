// elogctl: the cluster's observation port for tests and scripts (LLD §15.2, R-18). Output is
// machine-readable: exit codes, one JSON object, or one JSON object per line.
//
//     elogctl --coordinator HOST:PORT create-topic <name> -p P -r R [--min-isr N]
//     elogctl --coordinator HOST:PORT describe-topic <name>
//     elogctl --coordinator HOST:PORT dump <topic> <partition>
//     elogctl --coordinator HOST:PORT health [--brokers N] [--timeout-ms T]

#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "elog/client/rpc_connection.h"
#include "elog/protocol/app_value.h"
#include "elog/protocol/error_codes.h"
#include "elog/protocol/json_codec.h"
#include "elog/protocol/msgpack_codec.h"
#include "elog/protocol/record_batch.h"
#include "elog/tools/cli.h"

namespace {

using namespace elog;
using nlohmann::json;

int fail(const std::string& message, int code = 1) {
    std::fprintf(stderr, "elogctl: %s\n", message.c_str());
    return code;
}

Result<MetadataResponse> metadata(RpcConnection& coordinator, std::vector<std::string> topics) {
    auto frame = coordinator.call(encode(MetadataRequest{std::move(topics)}, 0));
    if (!frame.ok()) {
        return {frame.error(), frame.message()};
    }
    return decodeMetadataResponse(frame.value());
}

// One fetch from the partition's leader; credit 0 learns the HW without reading records.
Result<FetchPartitionResponse> fetch(const BrokerAddress& leader, const PartitionMetadata& p,
                                     const std::string& topic, Offset from, std::uint32_t credit) {
    RpcConnection broker(leader.host, leader.port);
    auto frame = broker.call(
        encode(FetchRequest{0, 4 << 20, credit, {{topic, p.partition, from, p.leaderEpoch}}}, 0));
    if (!frame.ok()) {
        return {frame.error(), frame.message()};
    }
    auto response = decodeFetchResponse(frame.value());
    if (!response.ok() || response.value().partitions.size() != 1) {
        return {ErrorCode::CORRUPT_MESSAGE, "bad fetch response"};
    }
    return response.value().partitions.front();
}

const BrokerAddress* findBroker(const MetadataResponse& m, BrokerId id) {
    for (const BrokerAddress& b : m.brokers) {
        if (b.id == id) {
            return &b;
        }
    }
    return nullptr;
}

int createTopic(RpcConnection& coordinator, const tools::Args& args) {
    const auto partitions = args.getInt("-p"), rf = args.getInt("-r"),
               minIsr = args.getInt("--min-isr");
    if (args.positional().size() != 2 || !partitions.ok() || !rf.ok() || !minIsr.ok()) {
        return fail("usage: create-topic <name> -p P -r R [--min-isr N]", 2);
    }
    auto frame = coordinator.call(
        encode(CreateTopicRequest{args.positional()[1], std::uint32_t(partitions.value()),
                                  std::uint32_t(rf.value()), std::uint32_t(minIsr.value())},
               0));
    if (!frame.ok()) {
        return fail(frame.message());
    }
    const auto response = decodeCreateTopicResponse(frame.value());
    const ErrorCode error = response.ok() ? response.value().error : response.error();
    return error == ErrorCode::NONE ? 0 : fail(std::string(errorName(error)));
}

int describeTopic(RpcConnection& coordinator, const tools::Args& args) {
    if (args.positional().size() != 2) {
        return fail("usage: describe-topic <name>", 2);
    }
    auto m = metadata(coordinator, {args.positional()[1]});
    if (!m.ok() || m.value().topics.size() != 1 || m.value().topics[0].error != ErrorCode::NONE) {
        return fail(m.ok() ? "unknown topic" : m.message());
    }
    const TopicMetadata& topic = m.value().topics[0];
    json out = {{"name", topic.name}, {"partitions", json::array()}};
    for (const PartitionMetadata& p : topic.partitions) {
        std::int64_t hw = -1;  // R-18: -1 if the leader cannot be asked
        if (const BrokerAddress* leader = findBroker(m.value(), p.leader)) {
            auto f = fetch(*leader, p, topic.name, 0, 0);
            if (f.ok() && f.value().error == ErrorCode::NONE) {
                hw = static_cast<std::int64_t>(f.value().highWatermark);
            }
        }
        out["partitions"].push_back({{"partition", p.partition},
                                     {"leader", p.leader},
                                     {"leaderEpoch", p.leaderEpoch},
                                     {"replicas", p.replicas},
                                     {"isr", p.isr},
                                     {"highWatermark", hw}});
    }
    std::cout << out.dump() << '\n';
    return 0;
}

// Prints every record of `batches` at or after `from`; returns the next offset to read.
Result<Offset> printRecords(std::span<const std::byte> batches, Offset from) {
    Offset next = from;
    while (batches.size() >= batch::kHeaderBytes) {
        const std::size_t size = batch::kRecordBatchOverhead + readBatchHeader(batches).batchLength;
        auto view = RecordBatchView::parse(batches.first(std::min(size, batches.size())));
        if (!view.ok()) {
            return {view.error(), view.message()};
        }
        view.value().forEachRecord([&](const Record& r) {
            if (r.offset < from) {
                return;
            }
            json line = {
                {"offset", r.offset}, {"key", nullptr}, {"pid", nullptr}, {"seq", nullptr}};
            if (r.key) {
                line["key"] =
                    std::string(reinterpret_cast<const char*>(r.key->data()), r.key->size());
            }
            if (auto value = decodeAppValue(r.value); value.ok()) {
                line["pid"] = value.value().pid;
                line["seq"] = value.value().seq;
            }
            std::cout << line.dump(-1, ' ', false, json::error_handler_t::replace) << '\n';
            next = r.offset + 1;
        });
        batches = batches.subspan(size);
    }
    return next;
}

int dump(RpcConnection& coordinator, const tools::Args& args) {
    const auto& pos = args.positional();
    if (pos.size() != 3 || pos[2].find_first_not_of("0123456789") != std::string::npos) {
        return fail("usage: dump <topic> <partition>", 2);
    }
    auto m = metadata(coordinator, {pos[1]});
    const std::uint32_t partition = std::stoul(pos[2]);
    if (!m.ok() || m.value().topics.size() != 1 || m.value().topics[0].error != ErrorCode::NONE ||
        partition >= m.value().topics[0].partitions.size()) {
        return fail("unknown topic or partition");
    }
    const PartitionMetadata& p = m.value().topics[0].partitions[partition];
    const BrokerAddress* leader = findBroker(m.value(), p.leader);
    if (!leader) {
        return fail("partition has no live leader");
    }
    auto start = fetch(*leader, p, pos[1], 0, 0);
    if (!start.ok() || start.value().error != ErrorCode::NONE) {
        return fail("cannot read the high watermark");
    }
    const Offset end = start.value().highWatermark;  // R-18: up to the HW at start
    for (Offset next = 0; next < end;) {
        auto f = fetch(*leader, p, pos[1], next, 10'000);
        if (!f.ok() || f.value().error != ErrorCode::NONE) {
            return fail("fetch failed at offset " + std::to_string(next));
        }
        auto printed = printRecords(f.value().records, next);
        if (!printed.ok() || printed.value() == next) {
            return fail("no progress at offset " + std::to_string(next));
        }
        next = printed.value();
    }
    return 0;
}

int health(RpcConnection& coordinator, const tools::Args& args) {
    const auto brokers = args.getInt("--brokers"), timeoutMs = args.getInt("--timeout-ms");
    if (!brokers.ok() || !timeoutMs.ok()) {
        return fail("usage: health [--brokers N] [--timeout-ms T]", 2);
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs.value());
    while (true) {
        auto m = metadata(coordinator, {});
        if (m.ok() && m.value().error == ErrorCode::NONE &&
            std::int64_t(m.value().brokers.size()) >= brokers.value()) {
            return 0;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            return fail(m.ok()
                            ? "only " + std::to_string(m.value().brokers.size()) + " live brokers"
                            : m.message());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

}  // namespace

int main(int argc, char** argv) {
    auto args = tools::parseArgs(argc, argv, 1,
                                 {{"--coordinator", "", true},
                                  {"-p", "0"},
                                  {"-r", "0"},
                                  {"--min-isr", "1"},
                                  {"--brokers", "1"},
                                  {"--timeout-ms", "0"}});
    if (!args.ok()) {
        return fail(args.message(), 2);
    }
    auto address = parseHostPort(args.value().get("--coordinator"));
    if (!address.ok()) {
        return fail(address.message(), 2);
    }
    RpcConnection coordinator(address.value().first, address.value().second,
                              std::chrono::milliseconds(5000));
    const std::string command =
        args.value().positional().empty() ? "" : args.value().positional()[0];
    if (command == "create-topic")
        return createTopic(coordinator, args.value());
    if (command == "describe-topic")
        return describeTopic(coordinator, args.value());
    if (command == "dump")
        return dump(coordinator, args.value());
    if (command == "health")
        return health(coordinator, args.value());
    return fail("unknown command '" + command + "' (create-topic, describe-topic, dump, health)",
                2);
}
