#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// BasicPageGuard
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::cout << "Constructor(Basic Page)" << std::endl;
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.already_unpin_ = true;
  // bpm_->FetchPage(page_->GetPageId());
  // that.Drop();
  // auto &&ref = that.page_;
  // page_ = std::move(ref);
  // that.~BasicPageGuard();
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr || already_unpin_) {  // 这代表这个PageGuard已经没用了，不应该再影响实际page内容
    std::cout << "already unpin or wrong drop()" << std::endl;
    return;
  }
  std::cout << "drop basic page: " << page_->GetPageId() << std::endl;
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);  // 先把这个page evictable掉，至少表明自己不再约束这个page的去留了
  // if (!bpm_->DeletePage(page_->GetPageId())) {  // 然后尝试去删掉这个page
  // BUSTUB_ASSERT("invalid delete when droping pageguard at id: {}.", page_->GetPageId());
  // }
  // bpm_->DeletePage(page_->GetPageId());  // 尝试删除
  // 无论上面的delete是否成功吧，至少我这边认为已经删掉了，即这一层对page的guard撤掉了
  // is_dirty_ = false;
  // bpm_ = nullptr;
  // page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // if (page_ != nullptr) {
  std::cout << "operator=(Basic Page)" << std::endl;
  this->Drop();
  // }
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.already_unpin_ = true;
  // bpm_->FetchPage(page_->GetPageId());
  // that.Drop();
  // that.~BasicPageGuard();
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  std::cout << "~ BasicPageGuard" << std::endl;
  this->Drop();
}  // NOLINT

// ReadPageGuard
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  std::cout << "Constructor(Read Page)" << std::endl;
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.already_unpin_ = true;
  // guard_.bpm_->FetchPage(guard_.page_->GetPageId());
  // that.Drop()
  // that.~ReadPageGuard();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  std::cout << "operator=(Read Page)" << std::endl;
  this->Drop();
  guard_.Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  // guard_.bpm_->FetchPage(guard_.page_->GetPageId());
  that.guard_.already_unpin_ = true;
  // that.~ReadPageGuard();
  return *this;
}

void ReadPageGuard::Drop() {
  std::cout << "drop read page: " << guard_.page_->GetPageId() << std::endl;
  guard_.page_->RUnlatch();
  // guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  std::cout << "~ ReadPageGuard" << std::endl;
  this->Drop();
}  // NOLINT

// WritePageGuard
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  std::cout << "Constructor(Write Page)" << std::endl;
  // wlatch_.lock();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  // guard_.bpm_->FetchPage(guard_.page_->GetPageId());
  that.guard_.already_unpin_ = true;
  // that.~WritePageGuard();
  // wlatch_.unlock();
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  std::cout << "operator=(Write Page)" << std::endl;
  // wlatch_.lock();
  this->Drop();
  guard_.Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  // guard_.bpm_->FetchPage(guard_.page_->GetPageId());
  that.guard_.already_unpin_ = true;
  // that.~WritePageGuard();
  // wlatch_.unlock();
  return *this;
}

void WritePageGuard::Drop() {
  // wlatch_.lock();
  std::cout << "drop write page: " << guard_.page_->GetPageId() << std::endl;
  guard_.page_->WUnlatch();
  // guard_.Drop();
  // wlatch_.unlock();
}

WritePageGuard::~WritePageGuard() {
  std::cout << "~ WritePageGuard" << std::endl;
  this->Drop();
}  // NOLINT

}  // namespace bustub
