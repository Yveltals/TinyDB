#pragma once
#include "db/db.h"
#include "db/dbformat.h"

namespace tinydb {

class Snapshot {
 public:
  Snapshot(SequenceNumber sequence_number)
      : sequence_number_(sequence_number) {}
  SequenceNumber sequence_number() const { return sequence_number_; }

 private:
  friend class SnapshotList;
  const SequenceNumber sequence_number_;
};

class SnapshotList {
 public:
  SnapshotList() = default;
  ~SnapshotList() {
    for (auto s : snapshots_) delete s;
  };

  bool empty() const { return snapshots_.empty(); }
  Snapshot* oldest() const { return snapshots_.front(); }
  Snapshot* newest() const { return snapshots_.front(); }
  Snapshot* New(SequenceNumber seq) {
    assert(empty() || newest()->sequence_number_ <= seq);
    auto snapshot = new Snapshot(seq);
    snapshots_.push_back(snapshot);
    return snapshot;
  }
  void Delete(const Snapshot* snapshot) {
    // TODO
  }

 private:
  std::list<Snapshot*> snapshots_;
};

} // namespace tinydb