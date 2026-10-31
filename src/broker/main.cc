// elog-broker: serves produce and fetch requests for the partitions it hosts (LLD §6).
//
//     elog-broker --set broker.id=1 [--config broker.properties] [--set key=value ...]

#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "elog/broker/broker_config.h"
#include "elog/broker/broker_server.h"
#include "elog/common/config.h"
#include "elog/common/logging.h"
#include "elog/common/version.h"

namespace {

using elog::ConfigKey;

struct CommandLine {
    std::optional<std::string> configFile;
    std::vector<std::string> overrides;  // "key=value"
    bool help = false;
    bool version = false;
};

void printUsage(std::FILE* out, const std::vector<ConfigKey>& schema) {
    std::fprintf(out,
                 "Usage: elog-broker [--config FILE] [--set key=value]... [--help] [--version]\n"
                 "\n"
                 "Settings come from defaults, then FILE (key=value lines), then --set.\n"
                 "Keys (LLD §12.1):\n");
    for (const ConfigKey& key : schema) {
        const std::string value = key.defaultValue ? "default " + *key.defaultValue : "required";
        std::fprintf(out, "  %-24s %s\n", key.name.c_str(), value.c_str());
    }
}

// Returns nullopt (after printing why) if the arguments are not understood.
std::optional<CommandLine> parseCommandLine(int argc, char** argv) {
    CommandLine cl;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        const bool hasValue = i + 1 < argc;
        if (arg == "--help" || arg == "-h") {
            cl.help = true;
        } else if (arg == "--version") {
            cl.version = true;
        } else if (arg == "--config" && hasValue) {
            cl.configFile = argv[++i];
        } else if (arg == "--set" && hasValue) {
            cl.overrides.emplace_back(argv[++i]);
        } else {
            std::fprintf(stderr, "elog-broker: unexpected argument '%s'\n", argv[i]);
            return std::nullopt;
        }
    }
    return cl;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<ConfigKey> schema = elog::brokerConfigSchema();
    const auto commandLine = parseCommandLine(argc, argv);
    if (!commandLine) {
        printUsage(stderr, schema);
        return 2;
    }
    if (commandLine->help) {
        printUsage(stdout, schema);
        return 0;
    }
    if (commandLine->version) {
        std::printf("elog-broker %s\n", std::string(elog::versionString()).c_str());
        return 0;
    }

    auto config = elog::Config::load(schema, commandLine->configFile, commandLine->overrides);
    if (!config.ok()) {
        std::fprintf(stderr, "elog-broker: %s\n", config.message().c_str());
        return 2;
    }
    const auto cfg = elog::BrokerConfig::from(config.value());

    const std::string node = "broker-" + std::to_string(cfg.brokerId);
    if (auto status = elog::logging::init(node, std::nullopt); !status.ok()) {
        std::fprintf(stderr, "elog-broker: %s\n", status.message().c_str());
        return 2;
    }
    // The effective configuration, logged once at startup (LLD §12).
    std::string settings;
    for (const auto& [key, value] : config.value().entries()) {
        settings += " " + key + "=" + value;
    }
    elog::logging::get("broker")->info("starting version={}{}", elog::versionString(), settings);

    elog::BrokerServer server(cfg);
    return server.run();
}
