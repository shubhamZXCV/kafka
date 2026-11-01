#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "elog/common/result.h"

struct sqlite3;
struct sqlite3_stmt;

namespace elog {

class Db;

// One prepared statement, owned and cached by Db (LLD §8.1). Parameters are bound by name
// (":g"). A bind error is kept and returned by the next step(), so callers check once.
// A statement that is still mid-way holds a read snapshot open: step() until it returns
// false, or call reset() after the rows you need.
class Stmt {
public:
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
    ~Stmt();

    void bind(const char* name, std::int64_t value);
    void bind(const char* name, std::string_view value);
    void bindNull(const char* name);

    // Runs one step: true if a row is ready to read, false once the statement is done.
    // Resets itself when done or on error.
    Result<bool> step();

    // Ends the current run early; the bindings are kept.
    void reset();

    std::int64_t columnInt(int column) const;
    std::string columnText(int column) const;
    bool columnIsNull(int column) const;

private:
    friend class Db;
    Stmt(sqlite3* db, sqlite3_stmt* stmt) : db_(db), stmt_(stmt) {}
    void resetForUse();  // reset() and clear the bindings

    sqlite3* db_;
    sqlite3_stmt* stmt_;
    std::string bindError_;
};

// A transaction that rolls back when destroyed unless commit() succeeded (LLD §8.5).
class Txn {
public:
    Txn(Txn&& other) noexcept;
    Txn(const Txn&) = delete;
    Txn& operator=(const Txn&) = delete;
    Txn& operator=(Txn&&) = delete;
    ~Txn();

    Status commit();

private:
    friend class Db;
    explicit Txn(Db& db) : db_(&db) {}

    Db* db_;  // null once committed or moved from
};

// The coordinator's SQLite database (LLD §8). open() applies the connection settings of
// §8.1 and creates the §8.2 schema on first use.
//
// Thread rules: opened by the coordinator event-loop thread and used only from it
// (LLD §11.3).
class Db {
public:
    static Result<std::unique_ptr<Db>> open(const std::string& path);

    Db(const Db&) = delete;
    Db& operator=(const Db&) = delete;
    ~Db();

    // Runs SQL without parameters, possibly several statements.
    Status exec(std::string_view sql);

    // The cached statement for sql, prepared on first use and reset for this use.
    Result<Stmt*> prepare(const std::string& sql);

    // BEGIN IMMEDIATE; the returned Txn rolls back unless committed.
    Result<Txn> begin();

    // Rows changed by the last INSERT, UPDATE or DELETE (LLD §8.3: 1 = commit accepted).
    std::int64_t changes() const;

private:
    explicit Db(sqlite3* db) : db_(db) {}

    sqlite3* db_;
    std::unordered_map<std::string, std::unique_ptr<Stmt>> statements_;
};

}  // namespace elog
