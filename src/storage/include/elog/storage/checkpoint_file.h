#pragma once

#include <map>
#include <string>

#include "elog/common/result.h"
#include "elog/common/types.h"

namespace elog {

// Replaces `path` with `contents` so that a crash leaves either the old file or the new one:
// write <path>.tmp, fsync it, rename it over <path>, fsync the directory (LLD §5.2.3).
Status writeFileAtomically(const std::string& path, const std::string& contents);

// recovery-point.checkpoint and high-watermark.checkpoint (LLD §5.2.3): a version line, a
// count line, then "<topic> <partition> <offset>" per partition.
class CheckpointFile {
public:
    static Status write(const std::string& path, const std::map<TopicPartition, Offset>& offsets);

    // A missing file is an empty map; a malformed one is CORRUPT_MESSAGE.
    static Result<std::map<TopicPartition, Offset>> read(const std::string& path);
};

// meta.properties (LLD §5.2.4): written on first start; afterwards the broker.id in it must
// match the configured one, which guards against mixed-up data directories.
Status checkOrWriteMetaProperties(const std::string& dataDir, BrokerId brokerId);

}  // namespace elog
