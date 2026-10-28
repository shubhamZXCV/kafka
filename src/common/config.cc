#include "elog/common/config.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <fstream>
#include <utility>

namespace elog {

namespace {

using KeyValue = std::pair<std::string, std::string>;

std::string_view trim(std::string_view s) {
    const auto isSpace = [](char c) { return c == ' ' || c == '\t' || c == '\r'; };
    while (!s.empty() && isSpace(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && isSpace(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

std::optional<std::int64_t> parseInt(std::string_view s) {
    std::int64_t value = 0;
    const auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), value);
    if (ec != std::errc{} || end != s.data() + s.size()) {
        return std::nullopt;
    }
    return value;
}

Result<KeyValue> parseKeyValue(std::string_view text) {
    const auto eq = text.find('=');
    if (eq == std::string_view::npos) {
        return {ErrorCode::INVALID_REQUEST, "expected key=value, got '" + std::string(text) + "'"};
    }
    const auto key = trim(text.substr(0, eq));
    if (key.empty()) {
        return {ErrorCode::INVALID_REQUEST, "empty key in '" + std::string(text) + "'"};
    }
    return KeyValue{std::string(key), std::string(trim(text.substr(eq + 1)))};
}

Result<std::vector<KeyValue>> readConfigFile(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        return {ErrorCode::INVALID_REQUEST, "cannot open config file " + path};
    }
    std::vector<KeyValue> entries;
    std::string line;
    for (int lineNo = 1; std::getline(in, line); ++lineNo) {
        const auto text = trim(std::string_view(line).substr(0, line.find('#')));
        if (text.empty()) {
            continue;
        }
        auto kv = parseKeyValue(text);
        if (!kv.ok()) {
            return {kv.error(), path + ":" + std::to_string(lineNo) + ": " + kv.message()};
        }
        entries.push_back(std::move(kv).value());
    }
    return entries;
}

Status validate(const ConfigKey& key, const std::string& value) {
    const auto fail = [&](const std::string& why) {
        return Status{ErrorCode::INVALID_REQUEST, "config " + key.name + "=" + value + ": " + why};
    };
    switch (key.type) {
        case ConfigKey::Type::Integer: {
            const auto n = parseInt(value);
            if (!n) {
                return fail("not an integer");
            }
            if (key.allowDisabled && *n == -1) {
                return okStatus();
            }
            if (*n < key.min || *n > key.max) {
                return fail("out of range [" + std::to_string(key.min) + ", " +
                            std::to_string(key.max) + "]");
            }
            return okStatus();
        }
        case ConfigKey::Type::Bool:
            if (value != "true" && value != "false") {
                return fail("expected true or false");
            }
            return okStatus();
        case ConfigKey::Type::Choice:
            if (std::find(key.choices.begin(), key.choices.end(), value) == key.choices.end()) {
                return fail("not one of the allowed values");
            }
            return okStatus();
        case ConfigKey::Type::String:
            return okStatus();
    }
    return okStatus();
}

}  // namespace

ConfigKey ConfigKey::integer(std::string name, std::int64_t def, std::int64_t min,
                             std::int64_t max) {
    ConfigKey key;
    key.name = std::move(name);
    key.type = Type::Integer;
    key.defaultValue = std::to_string(def);
    key.min = min;
    key.max = max;
    return key;
}

ConfigKey ConfigKey::integerOrDisabled(std::string name, std::int64_t def, std::int64_t min,
                                       std::int64_t max) {
    ConfigKey key = integer(std::move(name), def, min, max);
    key.allowDisabled = true;
    return key;
}

ConfigKey ConfigKey::requiredInteger(std::string name, std::int64_t min, std::int64_t max) {
    ConfigKey key = integer(std::move(name), 0, min, max);
    key.defaultValue = std::nullopt;
    return key;
}

ConfigKey ConfigKey::string(std::string name, std::string def) {
    ConfigKey key;
    key.name = std::move(name);
    key.type = Type::String;
    key.defaultValue = std::move(def);
    return key;
}

ConfigKey ConfigKey::boolean(std::string name, bool def) {
    ConfigKey key;
    key.name = std::move(name);
    key.type = Type::Bool;
    key.defaultValue = def ? "true" : "false";
    return key;
}

ConfigKey ConfigKey::choice(std::string name, std::string def, std::vector<std::string> choices) {
    ConfigKey key;
    key.name = std::move(name);
    key.type = Type::Choice;
    key.defaultValue = std::move(def);
    key.choices = std::move(choices);
    return key;
}

Result<Config> Config::load(const std::vector<ConfigKey>& schema,
                            const std::optional<std::string>& file,
                            const std::vector<std::string>& overrides) {
    // 1. Built-in defaults.
    Config config;
    for (const auto& key : schema) {
        if (key.defaultValue) {
            config.values_[key.name] = *key.defaultValue;
        }
    }
    const auto isKnown = [&](const std::string& name) {
        return std::any_of(schema.begin(), schema.end(),
                           [&](const ConfigKey& key) { return key.name == name; });
    };

    // 2. The config file, then 3. --set overrides; later sources win.
    std::vector<KeyValue> layered;
    if (file) {
        auto entries = readConfigFile(*file);
        if (!entries.ok()) {
            return {entries.error(), entries.message()};
        }
        layered = std::move(entries).value();
    }
    for (const auto& text : overrides) {
        auto kv = parseKeyValue(text);
        if (!kv.ok()) {
            return {kv.error(), "--set: " + kv.message()};
        }
        layered.push_back(std::move(kv).value());
    }
    for (auto& [name, value] : layered) {
        if (!isKnown(name)) {
            return {ErrorCode::INVALID_REQUEST, "unknown config key '" + name + "'"};
        }
        config.values_[name] = std::move(value);
    }

    // 4. Every key present and valid.
    for (const auto& key : schema) {
        const auto it = config.values_.find(key.name);
        if (it == config.values_.end()) {
            return {ErrorCode::INVALID_REQUEST, "missing required config key '" + key.name + "'"};
        }
        if (auto status = validate(key, it->second); !status.ok()) {
            return {status.error(), status.message()};
        }
    }
    return config;
}

std::int64_t Config::getInt(std::string_view key) const {
    const auto it = values_.find(key);
    assert(it != values_.end() && "config key not in schema");
    return parseInt(it->second).value();  // validated by load()
}

std::string Config::getString(std::string_view key) const {
    const auto it = values_.find(key);
    assert(it != values_.end() && "config key not in schema");
    return it->second;
}

bool Config::getBool(std::string_view key) const {
    return getString(key) == "true";
}

std::chrono::milliseconds Config::getMs(std::string_view key) const {
    return std::chrono::milliseconds(getInt(key));
}

}  // namespace elog
