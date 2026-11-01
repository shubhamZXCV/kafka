#include "elog/tools/cli.h"

#include <charconv>

namespace elog::tools {

Result<std::int64_t> Args::getInt(const std::string& name) const {
    const std::string& text = values_.at(name);
    std::int64_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size()) {
        return {ErrorCode::INVALID_REQUEST, name + ": not an integer: '" + text + "'"};
    }
    return value;
}

Result<Args> parseArgs(int argc, char** argv, int first, const std::vector<Flag>& flags) {
    Args args;
    std::map<std::string, const Flag*> known;
    for (const Flag& flag : flags) {
        known[flag.name] = &flag;
        args.values_[flag.name] = flag.isSwitch ? "0" : flag.defaultValue;
    }
    std::map<std::string, bool> given;
    for (int i = first; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.empty() || arg[0] != '-') {
            args.positional_.push_back(arg);
            continue;
        }
        const auto flag = known.find(arg);
        if (flag == known.end()) {
            return {ErrorCode::INVALID_REQUEST, "unknown flag " + arg};
        }
        if (flag->second->isSwitch) {
            args.values_[arg] = "1";
        } else if (i + 1 < argc) {
            args.values_[arg] = argv[++i];
        } else {
            return {ErrorCode::INVALID_REQUEST, arg + " needs a value"};
        }
        given[arg] = true;
    }
    for (const Flag& flag : flags) {
        if (flag.required && !given[flag.name]) {
            return {ErrorCode::INVALID_REQUEST, "missing " + flag.name};
        }
    }
    return args;
}

}  // namespace elog::tools
