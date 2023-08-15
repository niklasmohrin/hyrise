#pragma once

#include <array>
#include <compare>
#include <cstdint>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

#include "assert.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "small_min_heap.hpp"

namespace hyrise {

template <typename T, uint8_t sublist_count>
void multiway_inplace_merge(const std::span<T> data, const std::span<T> scratch, auto comparator) {
  const auto sublist_size = data.size() / sublist_count;

  DebugAssert(sublist_size > 0, "Why are you even merging?");

  const auto end_of_scratch_data = (sublist_count - 1) * sublist_size;
  std::ranges::move(data.subspan(0, end_of_scratch_data), scratch.begin());

  auto sublists = std::array<std::span<T>, sublist_count>{};
  for (auto i = 0; i < sublist_count - 1; ++i)
    sublists[i] = scratch.subspan(i * sublist_size, sublist_size);
  sublists[sublist_count - 1] = data.subspan(end_of_scratch_data);

  const auto sublist_compare = [&](auto lhs, auto rhs) {
    DebugAssert(!sublists[lhs].empty(), "lhs empty");
    DebugAssert(!sublists[rhs].empty(), "rhs empty");
    const auto value_ordering = comparator(sublists[lhs].front(), sublists[rhs].front());
    if (std::is_eq(value_ordering))
      return lhs < rhs;
    return std::is_lt(value_ordering);
  };

  auto heap = SmallMinHeap<sublist_count, uint8_t, decltype(sublist_compare)>(sublist_compare);
  for (auto i = static_cast<uint8_t>(0); i < sublist_count; ++i)
    heap.push(i);

  auto output_iterator = data.begin();
  while (heap.size() > 1) {
    const auto list_index = heap.pop();
    auto& list = sublists[list_index];
    *output_iterator++ = std::move(list.front());
    list = list.subspan(1);
    if (!list.empty())
      heap.push(list_index);
  }

  if (!heap.empty()) {
    const auto list_index = heap.pop();
    if (list_index < sublist_count - 1) {
      std::ranges::move(sublists[list_index], output_iterator);
    }
  } else {
    DebugAssert(output_iterator == data.end(), "Somehow we lost (or gained) some data while merging");
  }
}

template <typename T, uint8_t fan_out, size_t base_size>
void parallel_merge_sort(const std::span<T> data, const std::span<T> scratch, auto comparator) {
  static_assert(fan_out >= 2);

  if (data.size() <= base_size) {
    // NOTE: The "stable" is needed for tests (against sqlite) to pass, but I think that it is not actually required by
    //       the specification.
    std::ranges::stable_sort(
        data, [&comparator](const auto& lhs, const auto& rhs) { return std::is_lt(comparator(lhs, rhs)); });
    return;
  }

  const auto sublist_size = data.size() / fan_out;

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>(fan_out, nullptr);
  for (auto i = 0; i < fan_out; ++i) {
    tasks[i] = std::make_shared<JobTask>([data, scratch, i, sublist_size, &comparator]() {
      const auto start = i * sublist_size;
      const auto size = i + 1 < fan_out ? sublist_size : data.size() - (fan_out - 1) * sublist_size;
      parallel_merge_sort<T, fan_out>(data.subspan(start, size), scratch.subspan(start, size), comparator);
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  multiway_inplace_merge<T, fan_out>(data, scratch, comparator);
}

template <typename T, uint8_t fan_out = 2, size_t base_size = 1u << 10u>
void parallel_merge_sort(const std::span<T> data, auto comparator) {
  auto scratch = std::vector<T>(data.size());
  parallel_merge_sort<T, fan_out, base_size>(data, scratch, comparator);
}

}  // namespace hyrise
