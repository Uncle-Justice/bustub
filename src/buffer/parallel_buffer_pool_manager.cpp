//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), num_instances_(num_instances) {
  // Allocate and create individual BufferPoolManagerInstances
  BufferPoolManagerInstance *instance;
  for (uint32_t i = 0; i < num_instances_; i++) {
    instance = new BufferPoolManagerInstance(pool_size_, num_instances_, i, disk_manager, log_manager);
    instances_.push_back(instance);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (uint32_t i = 0; i < num_instances_; i++) {
    delete instances_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  std::scoped_lock clock_lock{latch_};
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  std::scoped_lock clock_lock{latch_};
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return instances_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  std::scoped_lock clock_lock{latch_};
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return instances_[page_id % num_instances_]->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock clock_lock{latch_};
  // Unpin page_id from responsible BufferPoolManagerInstance
  return instances_[page_id % num_instances_]->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  std::scoped_lock clock_lock{latch_};
  // Flush page_id from responsible BufferPoolManagerInstance
  return instances_[page_id % num_instances_]->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  std::scoped_lock clock_lock{latch_};
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  u_int32_t index = start_index_;
  Page *page = nullptr;
  do {
    page = instances_[index]->NewPage(page_id);
    if (page != nullptr) {
      start_index_ = (start_index_ + 1) % num_instances_;
      return page;
    }
    index = (index + 1) % num_instances_;
  } while (index != start_index_);
  start_index_ = (start_index_ + 1) % num_instances_;
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  std::scoped_lock clock_lock{latch_};
  // Delete page_id from responsible BufferPoolManagerInstance
  return instances_[page_id % num_instances_]->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  std::scoped_lock clock_lock{latch_};
  // flush all pages from all BufferPoolManagerInstances
  for (auto it : instances_) {
    it->FlushAllPages();
  }
}

}  // namespace bustub
