#pragma once

#include <array>
#include <compare>
#include <cstdint>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "assert.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "small_min_heap.hpp"

namespace hyrise {

template <typename Comparator, typename T>
concept ThreeWayComparator = requires(Comparator comparator, const T& lhs, const T& rhs) {
                               { comparator(lhs, rhs) } -> std::convertible_to<std::partial_ordering>;
                             };

namespace parallel_merge_sort_impl {

template <typename T, uint8_t sublist_count>
void multiway_merge_into(const std::span<T> input, const std::span<T> output, ThreeWayComparator<T> auto comparator) {
  const auto sublist_size = input.size() / sublist_count;

  DebugAssert(sublist_size > 0, "Why are you even merging?");

  auto sublists = std::array<std::span<T>, sublist_count>{};
  for (auto i = 0; i < sublist_count - 1; ++i) {
    sublists[i] = input.subspan(i * sublist_size, sublist_size);
  }
  const auto last_sublist_start = (sublist_count - 1) * sublist_size;
  sublists[sublist_count - 1] = input.subspan(last_sublist_start);

  const auto sublist_compare = [&](auto lhs, auto rhs) {
    DebugAssert(!sublists[lhs].empty(), "lhs empty");
    DebugAssert(!sublists[rhs].empty(), "rhs empty");
    const auto value_ordering = comparator(sublists[lhs].front(), sublists[rhs].front());
    if (std::is_eq(value_ordering))
      return lhs < rhs;
    return std::is_lt(value_ordering);
  };

  auto heap = SmallMinHeap<sublist_count, uint8_t, decltype(sublist_compare)>(sublist_compare);
  for (auto i = static_cast<uint8_t>(0); i < sublist_count; ++i) {
    heap.push(i);
  }

  auto output_iterator = output.begin();

  while (heap.size() > 1) {
    const auto list_index = heap.pop();
    auto& list = sublists[list_index];
    *output_iterator++ = std::move(list.front());
    list = list.subspan(1);
    if (!list.empty()) {
      heap.push(list_index);
    }
  }

  if (!heap.empty()) {
    const auto list_index = heap.pop();
    std::ranges::move(sublists[list_index], output_iterator);
  } else {
    DebugAssert(output_iterator == output.end(), "Somehow we lost (or gained) some data while merging");
  }
}

enum class OutputMode {
  InInput,
  InScratch,
};

template <typename T, uint8_t fan_out, size_t base_size, OutputMode output_mode>
void sort(const std::span<T> input, const std::span<T> scratch, ThreeWayComparator<T> auto comparator) {
  static_assert(fan_out >= 2);

  if (input.size() <= base_size) {
    // NOTE: The "stable" is needed for tests (against sqlite) to pass, but I think that it is not actually required by
    //       the specification.
    std::ranges::stable_sort(
        input, [&comparator](const auto& lhs, const auto& rhs) { return std::is_lt(comparator(lhs, rhs)); });

    if constexpr (output_mode == OutputMode::InScratch) {
      std::ranges::move(input, scratch.begin());
    }

    return;
  }

  const auto sublist_size = input.size() / fan_out;

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>(fan_out, nullptr);
  for (auto i = 0; i < fan_out; ++i) {
    tasks[i] = std::make_shared<JobTask>([input, scratch, i, sublist_size, &comparator]() {
      const auto start = i * sublist_size;
      const auto size = i + 1 < fan_out ? sublist_size : input.size() - (fan_out - 1) * sublist_size;

      constexpr auto other_output_mode =
          output_mode == OutputMode::InInput ? OutputMode::InScratch : OutputMode::InInput;

      sort<T, fan_out, base_size, other_output_mode>(input.subspan(start, size), scratch.subspan(start, size),
                                                     comparator);
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  if constexpr (output_mode == OutputMode::InInput) {
    multiway_merge_into<T, fan_out>(scratch, input, comparator);
  } else {
    multiway_merge_into<T, fan_out>(input, scratch, comparator);
  }
}

}  // namespace parallel_merge_sort_impl

template <typename T, uint8_t fan_out = 2, size_t base_size = 1u << 10u>
void parallel_inplace_merge_sort(const std::span<T> data, ThreeWayComparator<T> auto comparator) {
  using namespace parallel_merge_sort_impl;

  auto scratch = std::vector<T>(data.size());
  sort<T, fan_out, base_size, OutputMode::InInput>(data, scratch, comparator);
}

}  // namespace hyrise
