/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_ROCKSENGINE_H_
#define KVSTORE_ROCKSENGINE_H_

#include <gtest/gtest_prod.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/checkpoint.h>

#include "common/base/Base.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVIterator.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

/**
 * @brief Rocksdb range iterator, only scan data in range [start, end)
 */
class RocksRangeIter : public KVIterator {
 public:
  RocksRangeIter(rocksdb::Iterator* iter, rocksdb::Slice start, rocksdb::Slice end, size_t vIdLen)
      : iter_(iter), start_(start), end_(end), vIdLen_(vIdLen) {
    LOG(INFO) << "[qy-profiling]-[RocksRangeIter-create] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksRangeIter-create] iterator point to vertex. start: "
                    << start_.data() << ", end: " << end_.data() << ", partid: " << partid
                    << ", tagid: " << tag << ", vid: " << vid;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksRangeIter-create] iterator point to edge. start: "
                      << start_.data() << ", end: " << end_.data() << ", partId: " << partid
                      << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO)
              << "[qy-profiling]-[RocksRangeIter-create] iterator key not vertex or edge. start: "
              << start_.data() << ", end: " << end_.data();
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksRangeIter-create] iterator is invalid. start: "
                  << start_.data() << ", end: " << end_.data();
      }
    }
  }

  ~RocksRangeIter() {
    LOG(INFO) << "[qy-profiling]-[RocksRangeIter-finish] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      iter_->Prev();  // move to last key
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksRangeIter-finish] iterator point to vertex. start: "
                    << start_.data() << ", end: " << end_.data() << ", partid: " << partid
                    << ", tagid: " << tag << ", vid: " << vid;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksRangeIter-finish] iterator point to edge. start: "
                      << start_.data() << ", end: " << end_.data() << ", partId: " << partid
                      << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO)
              << "[qy-profiling]-[RocksRangeIter-finish] iterator key not vertex or edge. start: "
              << start_.data() << ", end: " << end_.data();
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksRangeIter-finish] iterator is invalid. start: "
                  << start_.data() << ", end: " << end_.data();
      }
    }
  }

  bool valid() const override {
    return !!iter_ && iter_->Valid() && (iter_->key().compare(end_) < 0);
  }

  void next() override {
    iter_->Next();
  }

  void prev() override {
    iter_->Prev();
  }

  folly::StringPiece key() const override {
    return folly::StringPiece(iter_->key().data(), iter_->key().size());
  }

  folly::StringPiece val() const override {
    return folly::StringPiece(iter_->value().data(), iter_->value().size());
  }

 private:
  std::unique_ptr<rocksdb::Iterator> iter_;
  rocksdb::Slice start_;
  rocksdb::Slice end_;
  size_t vIdLen_;
};

/**
 * @brief Rocksdb prefix iterator, only scan data starts with prefix
 */
class RocksPrefixIter : public KVIterator {
 public:
  RocksPrefixIter(rocksdb::Iterator* iter, rocksdb::Slice prefix, size_t vIdLen)
      : iter_(iter), prefix_(prefix), vIdLen_(vIdLen) {
    LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-create] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-create] iterator point to vertex. partid: "
                    << partid << ", tagid: " << tag << ", vid: " << vid;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-create] iterator point to edge. partId: "
                      << partid << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-create] iterator key not vertex or edge.";
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-create] iterator is invalid.";
      }
    }
  }

  ~RocksPrefixIter() {
    LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-finish] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      iter_->Prev();  // move to last key
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-finish] iterator point to vertex. partid: "
                    << partid << ", vid: " << vid << ", tagid: " << tag;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-finish] iterator point to edge. partId: "
                      << partid << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-finish] iterator key not vertex or edge.";
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksPrefixIter-finish] iterator is invalid.";
      }
    }
  }

  bool valid() const override {
    return !!iter_ && iter_->Valid() && (iter_->key().starts_with(prefix_));
  }

  void next() override {
    iter_->Next();
  }

  void prev() override {
    iter_->Prev();
  }

  folly::StringPiece key() const override {
    return folly::StringPiece(iter_->key().data(), iter_->key().size());
  }

  folly::StringPiece val() const override {
    return folly::StringPiece(iter_->value().data(), iter_->value().size());
  }

 protected:
  std::unique_ptr<rocksdb::Iterator> iter_;
  rocksdb::Slice prefix_;
  size_t vIdLen_;
};

/**
 * @brief Rocksdb iterator to scan all data
 */
class RocksCommonIter : public KVIterator {
 public:
  explicit RocksCommonIter(rocksdb::Iterator* iter, size_t vIdLen) : iter_(iter), vIdLen_(vIdLen) {
    LOG(INFO) << "[qy-profiling]-[RocksCommonIter-create] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksCommonIter-create] iterator point to vertex. partid: "
                    << partid << ", tagid: " << tag << ", vid: " << vid;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksCommonIter-create] iterator point to edge. partId: "
                      << partid << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO) << "[qy-profiling]-[RocksCommonIter-create] iterator key not vertex or edge.";
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksCommonIter-create] iterator is invalid.";
      }
    }
  }

  ~RocksCommonIter() {
    LOG(INFO) << "[qy-profiling]-[RocksCommonIter-finish] vIdLen: " << vIdLen_;
    if (vIdLen_ > 0) {
      iter_->Prev();  // move to last key
      if (iter_->Valid()) {
        auto key = this->key();
        if (NebulaKeyUtils::isTag(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto tag = NebulaKeyUtils::getTagId(vIdLen_, key);
          auto vid = NebulaKeyUtils::getVertexId(vIdLen_, key);
          LOG(INFO) << "[qy-profiling]-[RocksCommonIter-finish] iterator point to vertex. partid: "
                    << partid << ", tagid: " << tag << ", vid: " << vid;
        } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
          auto partid = NebulaKeyUtils::getPart(key);
          auto src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
          auto dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
          auto edge_type = NebulaKeyUtils::getEdgeType(vIdLen_, key);
          auto ranking = NebulaKeyUtils::getRank(vIdLen_, key);
          if (vIdLen_ == 8) {
            uint64_t src_id = 0, dst_id = 0;
            for (auto it = src.rbegin(); it != src.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              src_id = src_id * 256 + v;
            }
            for (auto it = dst.rbegin(); it != dst.rend(); it++) {
              uint8_t v = (uint8_t)(*it);
              dst_id = dst_id * 256 + v;
            }
            src = std::to_string(src_id);
            dst = std::to_string(dst_id);
            LOG(INFO) << "[qy-profiling]-[RocksCommonIter-finish] iterator point to edge. partId: "
                      << partid << ", edgeKey {src: " << src << " edge type: " << edge_type
                      << " ranking: " << ranking << " dst: " << dst << "}";
          }
        } else {
          LOG(INFO) << "[qy-profiling]-[RocksCommonIter-finish] iterator key not vertex or edge.";
        }
      } else {
        LOG(INFO) << "[qy-profiling]-[RocksCommonIter-finish] iterator is invalid.";
      }
    }
  }

  bool valid() const override {
    return !!iter_ && iter_->Valid();
  }

  void next() override {
    iter_->Next();
  }

  void prev() override {
    iter_->Prev();
  }

  folly::StringPiece key() const override {
    return folly::StringPiece(iter_->key().data(), iter_->key().size());
  }

  folly::StringPiece val() const override {
    return folly::StringPiece(iter_->value().data(), iter_->value().size());
  }

 protected:
  std::unique_ptr<rocksdb::Iterator> iter_;
  size_t vIdLen_;
};

/***************************************
 *
 * Implementation of WriteBatch
 *
 **************************************/
class RocksWriteBatch : public WriteBatch {
 private:
  rocksdb::WriteBatch batch_;

 public:
  RocksWriteBatch() : batch_(FLAGS_rocksdb_batch_size) {}

  virtual ~RocksWriteBatch() = default;

  nebula::cpp2::ErrorCode put(folly::StringPiece key, folly::StringPiece value) override {
    if (batch_.Put(toSlice(key), toSlice(value)).ok()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
  }

  nebula::cpp2::ErrorCode remove(folly::StringPiece key) override {
    if (batch_.Delete(toSlice(key)).ok()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
  }

  // Remove all keys in the range [start, end)
  nebula::cpp2::ErrorCode removeRange(folly::StringPiece start, folly::StringPiece end) override {
    if (batch_.DeleteRange(toSlice(start), toSlice(end)).ok()) {
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
  }

  rocksdb::WriteBatch* data() {
    return &batch_;
  }
};
/**
 * @brief An implementation of KVEngine based on Rocksdb
 *
 */
class RocksEngine : public KVEngine {
  FRIEND_TEST(RocksEngineTest, SimpleTest);

 public:
  /**
   * @brief Construct a new rocksdb instance
   *
   * @param spaceId
   * @param vIdLen Vertex id length, used for perfix bloom filter
   * @param dataPath Rocksdb data path
   * @param walPath Rocksdb wal path
   * @param mergeOp Rocksdb merge operation
   * @param cfFactory Rocksdb compaction filter factory
   * @param readonly Whether start as read only instance
   */
  RocksEngine(GraphSpaceID spaceId,
              int32_t vIdLen,
              const std::string& dataPath,
              const std::string& walPath = "",
              std::shared_ptr<rocksdb::MergeOperator> mergeOp = nullptr,
              std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory = nullptr,
              bool readonly = false);

  ~RocksEngine() {
    LOG(INFO) << "Release rocksdb on " << dataPath_;
  }

  void stop() override;

  /**
   * @brief Return path to a spaceId, e.g. "/DataPath/nebula/spaceId", usually it should contain two
   * subdir: data and wal.
   */
  const char* getDataRoot() const override {
    return dataPath_.c_str();
  }

  /**
   * @brief Return the wal path
   */
  const char* getWalRoot() const override {
    return walPath_.c_str();
  }

  /**
   * @brief Get the rocksdb snapshot
   *
   * @return const void* Pointer of rocksdb snapshot
   */
  const void* GetSnapshot() override {
    return db_->GetSnapshot();
  }

  /**
   * @brief Release the given rocksdb snapshot
   *
   * @param snapshot Pointer of rocksdb snapshot to release
   */
  void ReleaseSnapshot(const void* snapshot) override {
    db_->ReleaseSnapshot(reinterpret_cast<const rocksdb::Snapshot*>(snapshot));
  }

  /**
   * @brief return a WriteBatch object to do batch operation
   *
   * @return std::unique_ptr<WriteBatch>
   */
  std::unique_ptr<WriteBatch> startBatchWrite() override;

  /**
   * @brief write the batch operation into kv engine
   *
   * @param batch WriteBatch object
   * @param disableWAL Whether wal is disabled, only used in rocksdb
   * @param sync Whether need to sync when write, only used in rocksdb
   * @param wait Whether wait until write result, rocksdb would return incompelete if wait is false
   * in certain scenario
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                                           bool disableWAL,
                                           bool sync,
                                           bool wait) override;

  /*********************
   * Data retrieval
   ********************/
  /**
   * @brief Read a single key
   *
   * @param key Key to read
   * @param value Pointer of value
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode get(const std::string& key,
                              std::string* value,
                              const void* snapshot = nullptr) override;

  /**
   * @brief Read a list of keys
   *
   * @param keys Keys to read
   * @param values Pointers of value
   * @return std::vector<Status> Result status of each key, if key[i] does not exist, the i-th value
   * in return value would be Status::KeyNotFound
   */
  std::vector<Status> multiGet(const std::vector<std::string>& keys,
                               std::vector<std::string>* values) override;

  /**
   * @brief Get all results in range [start, end)
   *
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @param iter Iterator in range [start, end)
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode range(const std::string& start,
                                const std::string& end,
                                std::unique_ptr<KVIterator>* iter,
                                const size_t vIdLen = 0) override;

  /**
   * @brief Get all results with 'prefix' str as prefix.
   *
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix'
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode prefix(const std::string& prefix,
                                 std::unique_ptr<KVIterator>* iter,
                                 const void* snapshot = nullptr,
                                 const size_t vIdLen = 0) override;

  /**
   * @brief Get all results with 'prefix' str as prefix starting form 'start'
   *
   * @param start Start key, inclusive
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix' beginning from 'start'
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode rangeWithPrefix(const std::string& start,
                                          const std::string& prefix,
                                          std::unique_ptr<KVIterator>* iter,
                                          const size_t vIdLen = 0) override;

  /**
   * @brief Prefix scan with prefix extractor
   *
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix'
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode prefixWithExtractor(const std::string& prefix,
                                              const void* snapshot,
                                              std::unique_ptr<KVIterator>* storageIter,
                                              const size_t vIdLen = 0);

  /**
   * @brief Prefix scan without prefix extractor, use total order seek
   *
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix'
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode prefixWithoutExtractor(const std::string& prefix,
                                                 const void* snapshot,
                                                 std::unique_ptr<KVIterator>* storageIter,
                                                 const size_t vIdLen = 0);

  /**
   * @brief Scan all data in rocksdb
   *
   * @param iter Iterator of rocksdb
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode scan(std::unique_ptr<KVIterator>* iter, const size_t vIdLen = 0) override;

  /*********************
   * Data modification
   ********************/
  /**
   * @brief Write a single record
   *
   * @param key Key to write
   * @param value Value to write
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode put(std::string key, std::string value) override;

  /**
   * @brief Write a batch of records
   *
   * @param keyValues Key-value pairs to write
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode multiPut(std::vector<KV> keyValues) override;

  /**
   * @brief Remove a single key
   *
   * @param key Key to remove
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode remove(const std::string& key) override;

  /**
   * @brief Remove a batch of keys
   *
   * @param keys Keys to remove
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode multiRemove(std::vector<std::string> keys) override;

  /**
   * @brief Remove key in range [start, end)
   *
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode removeRange(const std::string& start, const std::string& end) override;

  /*********************
   * Non-data operation
   ********************/
  /**
   * @brief Write the part key into rocksdb for persistance
   *
   * @param partId
   * @param raftPeers partition raft peers, including peers created during balance which are not in
   * the meta
   */
  void addPart(PartitionID partId, const Peers& raftPeers = {}) override;

  /**
   * @brief Update part info. Could only update the persist peers info in balancing now.
   *
   * @param partId
   * @param raftPeer
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode updatePart(PartitionID partId, const Peer& raftPeer) override;

  /**
   * @brief Remove the part key from rocksdb
   *
   * @param partId
   */
  void removePart(PartitionID partId) override;

  /**
   * @brief Return all partitions in rocksdb instance by scanning system part key.
   *
   * @return std::vector<PartitionID>
   */
  std::vector<PartitionID> allParts() override;

  /**
   * @brief Retrun all the balancing part->raft peers in rocksdb engine by scanning system part key.
   *
   * @return std::map<Partition, Peers>
   */
  std::map<PartitionID, Peers> balancePartPeers() override;

  /**
   * @brief Return total partition numbers
   */
  int32_t totalPartsNum() override;

  /**
   * @brief Ingest external sst files
   *
   * @param files SST file path
   * @param verifyFileChecksum  Whether verify sst check-sum during ingestion
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode ingest(const std::vector<std::string>& files,
                                 bool verifyFileChecksum = false) override;

  /**
   * @brief Set config option
   *
   * @param configKey Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setOption(const std::string& configKey,
                                    const std::string& configValue) override;

  /**
   * @brief Set DB config option
   *
   * @param configKey Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setDBOption(const std::string& configKey,
                                      const std::string& configValue) override;

  /**
   * @brief Get engine property
   *
   * @param property Config name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::string>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::string> getProperty(const std::string& property) override;

  /**
   * @brief Do data compation in lsm tree
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode compact() override;

  /**
   * @brief Flush data in memtable into sst
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode flush() override;

  /**
   * @brief Call rocksdb backup, mainly for rocksdb PlainTable mounted on tmpfs/ramfs
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode backup() override;

  /*********************
   * Checkpoint operation
   ********************/
  /**
   * @brief Create a rocksdb check point
   *
   * @param checkpointPath
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode createCheckpoint(const std::string& checkpointPath) override;

  /**
   * @brief Backup the data of a table prefix, for meta backup
   *
   * @param path KV engine path
   * @param tablePrefix Table prefix
   * @param filter Data filter when iterate the table
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> Return the sst file path if
   * succeed, else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::string> backupTable(
      const std::string& path,
      const std::string& tablePrefix,
      std::function<bool(const folly::StringPiece& key)> filter) override;

 private:
  /**
   * @brief System part key, indicate which partitions in rocksdb instance
   *
   * @param partId
   * @return std::string
   */
  std::string partKey(PartitionID partId);

  /**
   * @brief System balance key, containing balancing info
   *
   * @param partId
   * @return std::string
   */
  std::string balanceKey(PartitionID partId);

  /**
   * @brief Open the rocksdb backup engine, mainly for rocksdb PlainTable mounted on tmpfs/ramfs
   *
   * @param spaceId
   */
  void openBackupEngine(GraphSpaceID spaceId);

 private:
  GraphSpaceID spaceId_;
  std::string dataPath_;
  std::string walPath_;
  std::unique_ptr<rocksdb::DB> db_{nullptr};
  std::string backupPath_;
  std::unique_ptr<rocksdb::BackupEngine> backupDb_{nullptr};
  int32_t partsNum_ = -1;
  size_t extractorLen_;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_ROCKSENGINE_H_
