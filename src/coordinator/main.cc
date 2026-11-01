// elog-coordinator: topics, broker liveness, consumer groups and committed offsets (LLD §7).
//
//     elog-coordinator [--config coordinator.properties] [--set key=value ...]

#include <pthread.h>
#include <signal.h>

#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "elog/common/config.h"
#include "elog/common/logging.h"
#include "elog/common/version.h"
#include "elog/coordinator/coordinator_config.h"
#include "elog/coordinator/coordinator_server.h"

namespace {

using elog::ConfigKey;

struct CommandLine {
    std::optional<std::string> configFile;
    std::vector<std::string> overrides;  // "key=value"
    bool help = false;
    bool version = false;
};

void printUsage(std::FILE* out, const std::vector<ConfigKey>& schema) {
    std::fprintf(
        out,
        "Usage: elog-coordinator [--config FILE] [--set key=value]... [--help] [--version]\n"
        "\n"
        "Settings come from defaults, then FILE (key=value lines), then --set.\n"
        "Keys (LLD §12.2):\n");
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
            std::fprintf(stderr, "elog-coordinator: unexpected argument '%s'\n", argv[i]);
            return std::nullopt;
        }
    }
    return cl;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<ConfigKey> schema = elog::coordinatorConfigSchema();
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
        std::printf("elog-coordinator %s\n", std::string(elog::versionString()).c_str());
        return 0;
    }

    auto config = elog::Config::load(schema, commandLine->configFile, commandLine->overrides);
    if (!config.ok()) {
        std::fprintf(stderr, "elog-coordinator: %s\n", config.message().c_str());
        return 2;
    }
    const auto cfg = elog::CoordinatorConfig::from(config.value());

    if (auto status = elog::logging::init("coordinator", std::nullopt); !status.ok()) {
        std::fprintf(stderr, "elog-coordinator: %s\n", status.message().c_str());
        return 2;
    }
    // The effective configuration, logged once at startup (LLD §12).
    std::string settings;
    for (const auto& [key, value] : config.value().entries()) {
        settings += " " + key + "=" + value;
    }
    elog::logging::get("coordinator")
        ->info("starting version={}{}", elog::versionString(), settings);

    // SIGINT and SIGTERM are blocked in every thread (threads inherit the mask) and taken by
    // one waiting thread, so shutdown() runs as ordinary code, not in a signal handler.
    // SIGUSR1 only wakes that thread if run() returns on its own.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    sigaddset(&signals, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    elog::CoordinatorServer server(cfg);
    std::thread signalWaiter([&] {
        int signal = 0;
        sigwait(&signals, &signal);
        if (signal != SIGUSR1) {
            elog::logging::get("coordinator")
                ->info("shutdown_requested signal={}", sigabbrev_np(signal));
            server.shutdown();
        }
    });
    const int exitCode = server.run();
    pthread_kill(signalWaiter.native_handle(), SIGUSR1);
    signalWaiter.join();
    return exitCode;
}
