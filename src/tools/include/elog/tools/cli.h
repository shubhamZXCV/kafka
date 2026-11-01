#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "elog/common/result.h"

namespace elog::tools {

struct Flag {
    std::string name;          // e.g. "--topic" or "-p"
    std::string defaultValue;  // ignored when required
    bool required = false;
    bool isSwitch = false;  // takes no value; "1" when given
};

// Flags and positional arguments of one tool invocation (LLD §15.2, R-18).
class Args {
public:
    const std::string& get(const std::string& name) const { return values_.at(name); }
    Result<std::int64_t> getInt(const std::string& name) const;
    bool has(const std::string& name) const { return values_.at(name) == "1"; }
    const std::vector<std::string>& positional() const { return positional_; }

private:
    friend Result<Args> parseArgs(int, char**, int, const std::vector<Flag>&);
    std::map<std::string, std::string> values_;
    std::vector<std::string> positional_;
};

// Parses argv[first..argc). An unknown flag, a missing value or a missing required flag is
// an INVALID_REQUEST error, which tools report with exit code 2.
Result<Args> parseArgs(int argc, char** argv, int first, const std::vector<Flag>& flags);

}  // namespace elog::tools
