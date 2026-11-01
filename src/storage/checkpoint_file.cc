#include "elog/storage/checkpoint_file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

namespace elog {

namespace {

constexpr int kCheckpointVersion = 0;

Status ioError(const std::string& what, const std::string& path, int err) {
    return {ErrorCode::UNKNOWN_SERVER_ERROR,
            what + " " + path + ": " + std::system_category().message(err)};
}

Status fsyncPath(const std::string& path, int flags) {
    const int fd = ::open(path.c_str(), flags | O_CLOEXEC);
    if (fd < 0) {
        return ioError("open", path, errno);
    }
    const int rc = ::fsync(fd);
    const int err = errno;
    ::close(fd);
    return rc == 0 ? okStatus() : ioError("fsync", path, err);
}

}  // namespace

Status writeFileAtomically(const std::string& path, const std::string& contents) {
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        out << contents;
        out.close();
        if (!out) {
            return {ErrorCode::UNKNOWN_SERVER_ERROR, "write " + tmp + " failed"};
        }
    }
    if (auto status = fsyncPath(tmp, O_RDONLY); !status.ok()) {
        return status;
    }
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        return ioError("rename", tmp, errno);
    }
    // The rename is durable only once the directory entry is.
    return fsyncPath(std::filesystem::path(path).parent_path().string(), O_RDONLY | O_DIRECTORY);
}

Status CheckpointFile::write(const std::string& path,
                             const std::map<TopicPartition, Offset>& offsets) {
    std::ostringstream out;
    out << kCheckpointVersion << '\n' << offsets.size() << '\n';
    for (const auto& [tp, offset] : offsets) {
        out << tp.topic << ' ' << tp.partition << ' ' << offset << '\n';
    }
    return writeFileAtomically(path, out.str());
}

Result<std::map<TopicPartition, Offset>> CheckpointFile::read(const std::string& path) {
    std::map<TopicPartition, Offset> offsets;
    std::ifstream in(path);
    if (!in) {
        return offsets;  // no checkpoint yet
    }
    int version = -1;
    std::size_t count = 0;
    if (!(in >> version >> count) || version != kCheckpointVersion) {
        return {ErrorCode::CORRUPT_MESSAGE, path + ": bad header"};
    }
    for (std::size_t i = 0; i < count; ++i) {
        TopicPartition tp;
        Offset offset = 0;
        if (!(in >> tp.topic >> tp.partition >> offset)) {
            return {ErrorCode::CORRUPT_MESSAGE, path + ": expected " + std::to_string(count) +
                                                    " entries, got " + std::to_string(i)};
        }
        offsets[tp] = offset;
    }
    return offsets;
}

Status checkOrWriteMetaProperties(const std::string& dataDir, BrokerId brokerId) {
    std::error_code ec;
    std::filesystem::create_directories(dataDir, ec);
    if (ec) {
        return {ErrorCode::UNKNOWN_SERVER_ERROR, "create " + dataDir + ": " + ec.message()};
    }
    const std::string path = dataDir + "/meta.properties";
    std::ifstream in(path);
    if (!in) {
        return writeFileAtomically(path, "version=1\nbroker.id=" + std::to_string(brokerId) + "\n");
    }
    for (std::string line; std::getline(in, line);) {
        if (line.rfind("broker.id=", 0) == 0) {
            if (line.substr(10) == std::to_string(brokerId)) {
                return okStatus();
            }
            return {ErrorCode::INVALID_REQUEST, path + " belongs to broker " + line.substr(10) +
                                                    ", not " + std::to_string(brokerId)};
        }
    }
    return {ErrorCode::CORRUPT_MESSAGE, path + ": no broker.id"};
}

}  // namespace elog
