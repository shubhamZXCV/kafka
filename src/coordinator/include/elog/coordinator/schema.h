#pragma once

#include "elog/common/result.h"

namespace elog {

class Db;

// The schema version this code reads and writes (LLD §8.2).
inline constexpr int kSchemaVersion = 1;

// Creates the LLD §8.2 tables in a new database, in one transaction. An existing database
// must already be at kSchemaVersion; anything else is an error (no migrations).
Status ensureSchema(Db& db);

}  // namespace elog
