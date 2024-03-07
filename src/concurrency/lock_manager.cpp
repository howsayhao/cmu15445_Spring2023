//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <memory>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /** lock_type should satisfy isolation_level, while table_node supports all lock-mode */
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ: {
      if (txn->GetState() == TransactionState::SHRINKING) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    } break;
    case IsolationLevel::READ_COMMITTED: {
      if (txn->GetState() == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    } break;
    case IsolationLevel::READ_UNCOMMITTED: {
      // required
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // allowed
      if (!(txn->GetState() == TransactionState::GROWING &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    } break;
    default:
      break;
  }

  /** add lock_request_queue to null table_lock_map_[oid](queue), then get the queue */
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);  // cv_.wait() below requires unique_lock type

  /** check already_lock, upgrade if not-same-but-allowed */
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    auto lock_request = *it;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        ThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);
      }
      // 1. Check the precondition of upgrade
      LockMode old_lock_mode = lock_request->lock_mode_;
      if (!((old_lock_mode == LockMode::INTENTION_SHARED) ||
            (old_lock_mode == LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED &&
             lock_mode != LockMode::INTENTION_EXCLUSIVE) ||
            (old_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
             (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) ||
            (old_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 2. Drop the current lock, reserve the upgrade position
      lock_request_queue->request_queue_.erase(it);
      DeleteTxnLockTable(txn, old_lock_mode, oid);  // ok even old_lock not-granted yet
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      lock_request_queue->request_queue_.emplace_back(
          new_lock_request);  // fifo, but grantallowed() can cover upgrade case
      // 3. Wait to get the new lock granted
      while (!GrantAllowed(txn, lock_request_queue, lock_mode)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_lock_request);
          lock.unlock();
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      new_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertTxnLockTable(txn, lock_mode, oid);
      return true;
    }
  }

  /** first-time for this TXN requesting lock of this resource, i.e. no upgrade */
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.emplace_back(lock_request);
  while (!GrantAllowed(txn, lock_request_queue, lock_mode)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      /** (zhhao) by gpt, notify_all will not release lock
       *  but probably life-cycle ends last stmt thus not inducing all-resleep-problem */
      lock.unlock();  // not sure, so i add a unlock here
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertTxnLockTable(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  /** ensure the transaction currently holds a lock on the resource it is attempting to unlock. */
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_.at(oid);
  table_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);
  auto it = lock_request_queue->request_queue_.begin();
  for (; it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      break;
    }
  }
  if (it == lock_request_queue->request_queue_.end()) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  /** unlocking a table should only be allowed if the transaction does not hold locks on any
   *  row on that table. */
  auto s_row_set = txn->GetSharedRowLockSet();
  auto x_row_set = txn->GetExclusiveRowLockSet();
  if (!((s_row_set->find(oid) == s_row_set->end() || s_row_set->at(oid).empty()) &&
        (x_row_set->find(oid) == x_row_set->end() || x_row_set->at(oid).empty()))) {
    ThrowAbort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  /** update txn state */
  switch ((*it)->lock_mode_) {
    case LockMode::SHARED: {
      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        txn->SetState(TransactionState::SHRINKING);
      }
    } break;
    case LockMode::EXCLUSIVE: {
      txn->SetState(TransactionState::SHRINKING);
    } break;
    default:
      break;
  }
  DeleteTxnLockTable(txn, (*it)->lock_mode_, oid);
  lock_request_queue->request_queue_.erase(it);

  /** unlocking a resource should also grant any new lock requests for the resource (if possible). */
  lock.unlock();
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  /** Row locking should not support Intention locks. */
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  /** also Checks whether txn has appropriate table lock for taking row lock */
  if (lock_mode == LockMode::EXCLUSIVE) {  // at least IX
    if (!(txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid))) {
      ThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  if (lock_mode == LockMode::SHARED) {  // at least IS
    if (!(txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableExclusiveLocked(oid) ||
          txn->IsTableSharedIntentionExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
          txn->IsTableIntentionSharedLocked(oid))) {
      ThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  /** lock_type should satisfy isolation_level, while table_node supports all lock-mode */
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::REPEATABLE_READ: {
      if (txn->GetState() == TransactionState::SHRINKING) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    } break;
    case IsolationLevel::READ_COMMITTED: {
      if (txn->GetState() == TransactionState::SHRINKING &&
          !(lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
        ThrowAbort(txn, AbortReason::LOCK_ON_SHRINKING);
      }
    } break;
    case IsolationLevel::READ_UNCOMMITTED: {
      // required
      if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      // allowed
      if (!(txn->GetState() == TransactionState::GROWING &&
            (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    } break;
    default:
      break;
  }

  /** add lock_request_queue to null table_lock_map_[oid](queue), then get the queue */
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);  // cv_.wait() below requires unique_lock type

  /** check already_lock, upgrade if not-same-but-allowed */
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    auto lock_request = *it;
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        ThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);
      }
      // 1. Check the precondition of upgrade
      LockMode old_lock_mode = lock_request->lock_mode_;
      if (!((old_lock_mode == LockMode::INTENTION_SHARED) ||
            (old_lock_mode == LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED &&
             lock_mode != LockMode::INTENTION_EXCLUSIVE) ||
            (old_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
             (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) ||
            (old_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE))) {
        ThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 2. Drop the current lock, reserve the upgrade position
      lock_request_queue->request_queue_.erase(it);
      DeleteTxnLockRow(txn, old_lock_mode, oid, rid);  // ok even old_lock not-granted yet
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
      lock_request_queue->request_queue_.emplace_back(
          new_lock_request);  // fifo, but grantallowed() can cover upgrade case
      // 3. Wait to get the new lock granted
      while (!GrantAllowed(txn, lock_request_queue, lock_mode)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_lock_request);
          lock.unlock();
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }
      new_lock_request->granted_ = true;
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      InsertTxnLockRow(txn, lock_mode, oid, rid);
      return true;
    }
  }

  /** first-time for this TXN requesting lock of this resource, i.e. no upgrade */
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.emplace_back(lock_request);
  while (!GrantAllowed(txn, lock_request_queue, lock_mode)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock.unlock();
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  InsertTxnLockRow(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  /** ensure the transaction currently holds a lock on the resource it is attempting to unlock. */
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock lock(lock_request_queue->latch_);
  auto it = lock_request_queue->request_queue_.begin();
  for (; it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      break;
    }
  }
  if (it == lock_request_queue->request_queue_.end()) {
    ThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  /** update txn state */
  if (!force) {
    switch ((*it)->lock_mode_) {
      case LockMode::SHARED: {
        if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
          txn->SetState(TransactionState::SHRINKING);
        }
      } break;
      case LockMode::EXCLUSIVE: {
        txn->SetState(TransactionState::SHRINKING);
      } break;
      default:
        break;
    }
  }
  DeleteTxnLockRow(txn, (*it)->lock_mode_, oid, rid);
  lock_request_queue->request_queue_.erase(it);

  /** unlocking a resource should also grant any new lock requests for the resource (if possible). */
  lock.unlock();
  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::ThrowAbort(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

void LockManager::DeleteTxnLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();  // need, since abort can effect;
  if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  } else {  // lock_mode == LockMode::EXCLUSIVE
    txn->GetExclusiveTableLockSet()->erase(oid);
  }
  txn->UnlockTxn();
}

void LockManager::DeleteTxnLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  txn->UnlockTxn();
}

void LockManager::InsertTxnLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  txn->LockTxn();
  if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  } else {  // lock_mode == LockMode::EXCLUSIVE
    txn->GetExclusiveTableLockSet()->insert(oid);
  }
  txn->UnlockTxn();
}

void LockManager::InsertTxnLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->find(oid) == txn->GetSharedRowLockSet()->end()) {
      txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<RID>()).first->second.insert(rid);
    } else {
      txn->GetSharedRowLockSet()->at(oid).insert(rid);
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (txn->GetExclusiveRowLockSet()->find(oid) == txn->GetExclusiveRowLockSet()->end()) {
      txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<RID>()).first->second.insert(rid);
    } else {
      txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
    }
  }
  txn->UnlockTxn();
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::INTENTION_SHARED: {
      if (l2 == LockMode::EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::INTENTION_EXCLUSIVE: {
      if (l2 != LockMode::INTENTION_SHARED && l2 != LockMode::INTENTION_EXCLUSIVE) {
        return false;
      }
    } break;
    case LockMode::SHARED: {
      if (l2 != LockMode::SHARED && l2 != LockMode::INTENTION_SHARED) {
        return false;
      }
    } break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      if (l2 != LockMode::INTENTION_SHARED) {
        return false;
      }
    } break;
    default:
      return false;  // EXCLUSIVE
  }
  return true;
}

auto LockManager::GrantAllowed(Transaction *txn, const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                               LockMode lock_mode) -> bool {
  /** Consider Upgrading */
  for (auto const &grant_lock_request : lock_request_queue->request_queue_) {
    if (grant_lock_request->granted_) {
      if (!AreLocksCompatible(grant_lock_request->lock_mode_, lock_mode)) {
        return false;
      }
      continue;
    }
    // break; // fifo
    /** allow compatible lock requests granted at the same time may generate hole
     *  that can destroy fifo, allowing wrong grant for a upgrading's compatible check */
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    return true;
  }
  if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
    return false;
  }

  /** If there are multiple compatible lock requests, all should be granted at the same time
   *  as long as FIFO is honoured. */
  for (auto const &yet_grant_lock_request : lock_request_queue->request_queue_) {
    if (yet_grant_lock_request->txn_id_ == txn->GetTransactionId()) {
      return true;
    }
    if (!yet_grant_lock_request->granted_ && !AreLocksCompatible(yet_grant_lock_request->lock_mode_, lock_mode)) {
      return false;  // may get hole that destroy fifo ?  // TODO
    }
  }
  std::cout << "unexpected when grant-allow" << std::endl;
  return false;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
