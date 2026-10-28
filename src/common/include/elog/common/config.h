#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "elog/common/result.h"

namespace elog {

// One configuration key a binary accepts: its type, default and allowed values (LLD §12).
// Build them with the factory functions, e.g.
//     ConfigKey::integer("segment.bytes", 67'108'864, 1 << 20, 1 << 30)
struct ConfigKey {
    enum class Type { Integer, String, Bool, Choice };

    std::string name;
    Type type = Type::String;
    std::optional<std::string> defaultValue;  // nullopt: the key is required
    std::int64_t min = 0;                     // Integer only
    std::int64_t max = 0;                     // Integer only
    bool allowDisabled = false;               // Integer only: -1 is also accepted ("off")
    std::vector<std::string> choices;         // Choice only

    static ConfigKey integer(std::string name, std::int64_t def, std::int64_t min,
                             std::int64_t max);
    // Accepts -1 (disabled) or a value in [min, max], e.g. retention.ms.
    static ConfigKey integerOrDisabled(std::string name, std::int64_t def, std::int64_t min,
                                       std::int64_t max);
    static ConfigKey requiredInteger(std::string name, std::int64_t min, std::int64_t max);
    static ConfigKey string(std::string name, std::string def);
    static ConfigKey boolean(std::string name, bool def);
    static ConfigKey choice(std::string name, std::string def, std::vector<std::string> choices);
};

// The validated, effective configuration of one process.
//
// Precedence (LLD §12): built-in default < --config file < --set overrides. An unknown key, a
// missing required key or an out-of-range value makes load() fail, so a typo is a startup
// error instead of a silently ignored setting.
//
// Immutable after load(); safe to read from any thread.
class Config {
public:
    // `file` holds key=value lines ('#' starts a comment). Each override is "key=value".
    static Result<Config> load(const std::vector<ConfigKey>& schema,
                               const std::optional<std::string>& file,
                               const std::vector<std::string>& overrides);

    // Getters take a key from the schema with the matching type; anything else is a bug.
    std::int64_t getInt(std::string_view key) const;
    std::string getString(std::string_view key) const;
    bool getBool(std::string_view key) const;
    std::chrono::milliseconds getMs(std::string_view key) const;

    // All keys and their effective values, sorted by key: logged at startup and written
    // to run.json (IF-11).
    const std::map<std::string, std::string, std::less<>>& entries() const { return values_; }

private:
    std::map<std::string, std::string, std::less<>> values_;
};

}  // namespace elog
