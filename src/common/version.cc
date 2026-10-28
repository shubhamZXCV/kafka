#include "elog/common/version.h"

namespace elog {

std::string_view versionString() {
    return ELOG_VERSION;
}

}  // namespace elog
