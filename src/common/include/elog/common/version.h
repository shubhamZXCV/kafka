#pragma once

#include <string_view>

namespace elog {

// Project version from CMake (e.g. "0.1.0"). Printed by every binary's --version.
std::string_view versionString();

}  // namespace elog
