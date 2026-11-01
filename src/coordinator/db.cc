#include "elog/coordinator/db.h"

#include <utility>

#include <sqlite3.h>

#include "elog/common/logging.h"
#include "elog/coordinator/schema.h"

namespace elog {

namespace {

// LLD §8.1. busy_timeout 0: one connection, so waiting for a lock would hide a bug.
constexpr std::string_view kConnectionSettings = R"sql(
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 0;
)sql";

std::string sqliteMessage(sqlite3* db, std::string_view what) {
    return std::string(what) + ": " + sqlite3_errmsg(db);
}

}  // namespace

// --- Stmt ------------------------------------------------------------------------------

Stmt::~Stmt() {
    sqlite3_finalize(stmt_);
}

void Stmt::bind(const char* name, std::int64_t value) {
    const int index = sqlite3_bind_parameter_index(stmt_, name);
    if (index == 0 || sqlite3_bind_int64(stmt_, index, value) != SQLITE_OK) {
        bindError_ = std::string("bind ") + name;
    }
}

void Stmt::bind(const char* name, std::string_view value) {
    const int index = sqlite3_bind_parameter_index(stmt_, name);
    if (index == 0 || sqlite3_bind_text(stmt_, index, value.data(), static_cast<int>(value.size()),
                                        SQLITE_TRANSIENT) != SQLITE_OK) {
        bindError_ = std::string("bind ") + name;
    }
}

void Stmt::bindNull(const char* name) {
    const int index = sqlite3_bind_parameter_index(stmt_, name);
    if (index == 0 || sqlite3_bind_null(stmt_, index) != SQLITE_OK) {
        bindError_ = std::string("bind ") + name;
    }
}

Result<bool> Stmt::step() {
    if (!bindError_.empty()) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, bindError_ + ": no such parameter"};
    }
    const int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
        return true;
    }
    std::string message = rc == SQLITE_DONE ? std::string() : sqliteMessage(db_, "step");
    sqlite3_reset(stmt_);
    if (rc != SQLITE_DONE) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, std::move(message)};
    }
    return false;
}

void Stmt::reset() {
    sqlite3_reset(stmt_);
}

std::int64_t Stmt::columnInt(int column) const {
    return sqlite3_column_int64(stmt_, column);
}

std::string Stmt::columnText(int column) const {
    const auto* text = reinterpret_cast<const char*>(sqlite3_column_text(stmt_, column));
    return text ? std::string(text, sqlite3_column_bytes(stmt_, column)) : std::string();
}

bool Stmt::columnIsNull(int column) const {
    return sqlite3_column_type(stmt_, column) == SQLITE_NULL;
}

void Stmt::resetForUse() {
    reset();
    sqlite3_clear_bindings(stmt_);
    bindError_.clear();
}

// --- Txn -------------------------------------------------------------------------------

Txn::Txn(Txn&& other) noexcept : db_(std::exchange(other.db_, nullptr)) {}

Txn::~Txn() {
    if (db_) {
        if (auto status = db_->exec("ROLLBACK"); !status.ok()) {
            logging::get("store")->error("rollback_failed error=\"{}\"", status.message());
        }
    }
}

Status Txn::commit() {
    auto status = db_->exec("COMMIT");
    if (status.ok()) {
        db_ = nullptr;  // nothing left to roll back
    }
    return status;  // on failure the destructor rolls back (LLD §8.5)
}

// --- Db --------------------------------------------------------------------------------

Result<std::unique_ptr<Db>> Db::open(const std::string& path) {
    sqlite3* handle = nullptr;
    const int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
    if (sqlite3_open_v2(path.c_str(), &handle, flags, nullptr) != SQLITE_OK) {
        std::string message = sqliteMessage(handle, "open " + path);
        sqlite3_close(handle);
        return {ErrorCode::UNKNOWN_SERVER_ERROR, std::move(message)};
    }
    std::unique_ptr<Db> db(new Db(handle));
    if (auto status = db->exec(kConnectionSettings); !status.ok()) {
        return {status.error(), status.message()};
    }
    if (auto status = ensureSchema(*db); !status.ok()) {
        return {status.error(), status.message()};
    }
    return db;
}

Db::~Db() {
    statements_.clear();  // finalize every statement before closing
    sqlite3_close(db_);
}

Status Db::exec(std::string_view sql) {
    char* message = nullptr;
    const std::string text(sql);
    if (sqlite3_exec(db_, text.c_str(), nullptr, nullptr, &message) != SQLITE_OK) {
        Status error{ErrorCode::UNKNOWN_SERVER_ERROR,
                     std::string("exec: ") + (message ? message : "unknown error")};
        sqlite3_free(message);
        return error;
    }
    return okStatus();
}

Result<Stmt*> Db::prepare(const std::string& sql) {
    if (auto it = statements_.find(sql); it != statements_.end()) {
        it->second->resetForUse();
        return it->second.get();
    }
    sqlite3_stmt* handle = nullptr;
    if (sqlite3_prepare_v3(db_, sql.c_str(), -1, SQLITE_PREPARE_PERSISTENT, &handle, nullptr) !=
        SQLITE_OK) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, sqliteMessage(db_, "prepare")};
    }
    auto& stmt = statements_[sql];
    stmt.reset(new Stmt(db_, handle));
    return stmt.get();
}

Result<Txn> Db::begin() {
    if (auto status = exec("BEGIN IMMEDIATE"); !status.ok()) {
        return {status.error(), status.message()};
    }
    return Txn(*this);
}

std::int64_t Db::changes() const {
    return sqlite3_changes64(db_);
}

}  // namespace elog
