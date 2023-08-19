#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// BasicPageGuard
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::cout << "Constructor(Basic Page)" << std::endl;
  // 自赋值
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  // guard_.already_unlock_ = false;
  // 将以前的page guard无效化
  that.page_ = nullptr;  // 使其basic page无效化
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {  // 这代表这个PageGuard已经没用了，不应该再影响实际page内容
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
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // if (page_ != nullptr) {
  std::cout << "operator=(Basic Page)" << std::endl;
  Drop();
  // }
  // 自赋值
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  // guard_.already_unlock_ = false;
  // 将以前的page guard无效化
  that.page_ = nullptr;  // 回收时不用unpin
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  std::cout << "~ BasicPageGuard" << std::endl;
  this->Drop();
}  // NOLINT

// ReadPageGuard
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  std::cout << "Constructor(Read Page)" << std::endl;
  // 自赋值
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  already_unlock_ = false;
  // 将以前的page guard无效化
  that.already_unlock_ = true;  // 回收时不用解锁
  that.guard_.page_ = nullptr;  // 回收时不用unpin
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  std::cout << "operator=(Read Page)" << std::endl;
  // 对原page进行unpin和解锁
  this->Drop();
  // 自赋值
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  already_unlock_ = false;
  // guard_.bpm_->FetchPage(guard_.page_->GetPageId());
  // 将以前的page guard无效化
  that.already_unlock_ = true;  // 使其外壳的read page无效化
  that.guard_.page_ = nullptr;  // 使其basic page无效化，使得跳过外层析构的同时还能跳过内层析构
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {  // 测试用例中有直接Drop()的情况，那么此时的Drop()应该要包括BasicPageGuard的Drop()
    std::cout << "read drop() already finished!!!!!" << std::endl;
    // 析构由外而内，进入这种情况就默认不用Drop()直接析构结束了，即已经Drop()过了
  } else if (!already_unlock_) {
    std::cout << "drop read page: " << guard_.page_->GetPageId() << std::endl;
    already_unlock_ = true;
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
  // guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  std::cout << "~ ReadPageGuard" << std::endl;
  // if (!already_unlock_) {
  this->Drop();
  // }
}  // NOLINT

// WritePageGuard
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  std::cout << "Constructor(Write Page)" << std::endl;
  // 自赋值
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  already_unlock_ = false;
  // 将以前的page guard无效化
  that.already_unlock_ = true;  // 使其外壳的read page无效化
  that.guard_.page_ = nullptr;  // 使其basic page无效化
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  std::cout << "operator=(Write Page)" << std::endl;
  // 清除以前的page_guard：构造由内而外，析构由外而内
  this->Drop();
  // 自赋值
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  already_unlock_ = false;
  // 将以前的page guard无效化
  that.already_unlock_ = true;  // 使其外壳的read page无效化
  that.guard_.page_ = nullptr;  // 使其basic page无效化
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {  // 测试用例中有直接Drop()的情况，那么此时的Drop()应该要包括BasicPageGuard的Drop()
    std::cout << "write drop() already finished!!!!!" << std::endl;
  } else if (!already_unlock_) {
    std::cout << "drop write page: " << guard_.page_->GetPageId() << std::endl;
    already_unlock_ = true;
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() {
  std::cout << "~ WritePageGuard" << std::endl;
  // if (!already_unlock_) {
  // guard_.Drop();
  this->Drop();
  // }
}  // NOLINT

}  // namespace bustub
