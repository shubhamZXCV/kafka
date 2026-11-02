# Wire Protocol Reference

The protocol as built (`src/protocol`). The design and its reasons are in the LLD
(§4); this page is what you need to talk to a broker or the coordinator, checked against the
codecs: every field name in LLD §4.6 appears in them and vice versa.

## Connections

- TCP, one request in flight per connection: send a frame, read the response, repeat. A
  slow server therefore slows its clients down (backpressure, LLD §6.5).
- The coordinator listens on 9000 and brokers on 9101 by default (`listen.port`).
- A malformed frame (bad length, magic or CRC) closes the connection; the process keeps
  running (NFR-SEC-02).

## Frame

All integers are big-endian.

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 4 | `frameLength` | bytes after this field: 12 + body + 4; at most `max.frame.bytes` (8 MiB), checked before allocating |
| 4 | 1 | `magic` | `0xE1` |
| 5 | 1 | `version` | `1` |
| 6 | 2 | `apiKey` | see below |
| 8 | 4 | `correlationId` | chosen by the client, echoed back |
| 12 | 1 | `encoding` | `0` JSON, `1` MessagePack |
| 13 | 1 | `flags` | bit 0 set on responses |
| 14 | 2 | reserved | `0` |
| 16 | n | body | JSON object or MessagePack map |
| 16 + n | 4 | `crc32c` | Castagnoli CRC of bytes 4 … 16 + n − 1 |

Code: `frame.h` / `frame.cc` (`encodeFrame`, `readFrame`).

## APIs

| Key | API | Caller → receiver | Body | Served by |
|---|---|---|---|---|
| 1 | `Metadata` | client, broker → coordinator | JSON | `MetadataManager` |
| 2 | `CreateTopic` | `elogctl` → coordinator | JSON | `MetadataManager` |
| 3 | `RegisterBroker` | broker → coordinator | JSON | `BrokerLiveness` |
| 4 | `BrokerHeartbeat` | broker → coordinator | JSON | `BrokerLiveness` |
| 5 | `AlterIsr` | leader → coordinator | JSON | `MetadataManager` |
| 6 | `LeaderAndIsr` | coordinator → broker | JSON | `ReplicaManager` |
| 10 | `Produce` | producer → leader | MessagePack | `ProduceHandler` |
| 11 | `Fetch` | consumer → leader | MessagePack | `FetchHandler` |
| 12 | `ReplicaFetch` | follower → leader | MessagePack | `ReplicaFetchHandler` |
| 13 | `OffsetsForLeaderEpoch` | follower → leader | JSON | `Replica::endOffsetForEpoch` |
| 20 | `JoinGroup` | consumer → coordinator | JSON | `GroupManager` |
| 21 | `Heartbeat` | consumer → coordinator | JSON | `GroupManager` |
| 22 | `LeaveGroup` | consumer → coordinator | JSON | `GroupManager` |
| 23 | `OffsetCommit` | consumer → coordinator | JSON | `OffsetManager` |
| 24 | `OffsetFetch` | consumer → coordinator | JSON | `OffsetManager` |
| 25 | `DescribeGroup` | `elogctl` → coordinator | JSON | `GroupManager` |

Message bodies: LLD §4.6 has every request and response with examples. Codecs:
`json_codec.cc` (metadata and broker control), `replication_codec.cc` (`AlterIsr`,
`OffsetsForLeaderEpoch`), `group_codec.cc` (groups and offsets), `msgpack_codec.cc` (data
plane). Arrays longer than 10 000 entries are rejected with `INVALID_REQUEST`.

Details a client must know:

- Every response has a top-level `error`; per-partition operations also have one per entry.
- `Produce` carries one record batch per partition, and its reply comes only once the batch
  is committed (on every ISR member).
- `Fetch` returns at most `credit` records in total, only below the high watermark. An
  `OFFSET_OUT_OF_RANGE` reply carries `highWatermark` and `logStartOffset`, so the client can
  reset (R-19).
- `ReplicaFetch`'s `fetchOffset` is the follower's log end, and acknowledges everything below
  it.
- `JoinGroup` replies only when the rebalance completes; its `assignor` tells the client
  whether the assignment is eager (`roundrobin`) or not (`sticky`).
- `OffsetCommit` entries carry the partition's ownership `epoch`; a stale one is refused per
  partition with `STALE_EPOCH` (fencing, LLD §8.3).
- Member IDs are `<clientId>-<16 hex digits>` (R-20).

## Error codes

| Code | Name | Retry? | Meaning |
|---|---|---|---|
| −1 | `NETWORK_ERROR` | reconnect | local only, never sent: connect failed, peer closed, timeout |
| 0 | `NONE` | | success |
| 1 | `CORRUPT_MESSAGE` | reconnect | bad magic, CRC or truncated frame |
| 2 | `UNSUPPORTED_VERSION` | no | unknown protocol version |
| 3 | `INVALID_REQUEST` | no | schema violation |
| 4 | `UNKNOWN_TOPIC_OR_PARTITION` | yes | not known to the receiver (yet) |
| 5 | `NOT_LEADER` | yes | refresh metadata |
| 6 | `STALE_EPOCH` | no | offset commit fenced: the partition has a newer owner |
| 7 | `FENCED_LEADER_EPOCH` | yes | the request's leader epoch is older than the receiver's |
| 8 | `UNKNOWN_MEMBER` | no | rejoin with an empty member ID |
| 9 | `ILLEGAL_GENERATION` | no | rejoin |
| 10 | `REBALANCE_IN_PROGRESS` | yes | commit, then rejoin |
| 11 | `OFFSET_OUT_OF_RANGE` | no | fetch offset outside [log start, end] |
| 12 | `NOT_ENOUGH_REPLICAS` | yes | ISR below `min.insync.replicas` |
| 13 | `THROTTLED` | yes | bounded resource full; `throttleMs` says how long to wait |
| 14 | `REQUEST_TIMED_OUT` | yes | produce not committed in time |
| 15 | `INVALID_PARTITION_COUNT` | no | topic exists with another partition count |
| 16 | `STALE_BROKER_INCARNATION` | no | message from a broker's earlier incarnation |
| 99 | `UNKNOWN_SERVER_ERROR` | no | a bug; logged with context |

Code: `common/error_code.h` (values), `protocol/error_codes.cc` (names, retriability).

## Record batch

The same bytes on the wire and on disk; the leader stamps `baseOffset` and `leaderEpoch`
in place, which the CRC does not cover.

| Offset | Size | Field | Set by |
|---|---|---|---|
| 0 | 8 | `baseOffset` | leader (producer sends 0) |
| 8 | 4 | `batchLength`: bytes after this field | producer |
| 12 | 4 | `leaderEpoch` | leader (producer sends 0) |
| 16 | 1 | `magic` = 1 | producer |
| 17 | 4 | `crc32c` of bytes 21 … end | producer |
| 21 | 2 | `attributes` = 0 | producer |
| 23 | 4 | `lastOffsetDelta` = records − 1 | producer |
| 27 | 8 | `firstTimestampMs` | producer |
| 35 | 8 | `maxTimestampMs` | producer |
| 43 | 4 | `recordCount` | producer |
| 47 | … | records | producer |

Each record is a MessagePack array `[offsetDelta, timestampDelta, key (bin or nil), value
(bin)]`. `elog-producer` writes values as a map `{pid, seq, ts, pad}` (`app_value.h`), which
the analysis reads back through `elogctl dump`.

Code: `record_batch.h` (`RecordBatchView`, `RecordBatchBuilder`, `stampBatch`, `sliceBatch`).
