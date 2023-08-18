#pragma once

#include <array>
#include <compare>  // NOLINT(build/include_order)
#include <span>     // NOLINT(build/include_order)
#include <vector>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

namespace hyrise::window_function_evaluator {

// Reverses the ordering (used for sorting DESC).
std::weak_ordering reverse(std::weak_ordering ordering);
// Comparator function for two AllTypeVariants.
std::weak_ordering compare_with_null_equal(const AllTypeVariant& lhs, const AllTypeVariant& rhs);
// Comparator function for spans of AllTypeVariants.
std::weak_ordering compare_with_null_equal(std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs);

// Comparator function for spans of AllTypeVariants with option for reversed columns (needed for order-by column sort modes).
std::weak_ordering compare_with_null_equal(std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs,
                                           auto is_column_reversed) {
  DebugAssert(lhs.size() == rhs.size(), "Tried to compare rows with different column counts.");

  const auto lhs_size = lhs.size();
  for (auto column_index = 0u; column_index < lhs_size; ++column_index) {
    const auto element_ordering = compare_with_null_equal(lhs[column_index], rhs[column_index]);
    if (element_ordering != std::weak_ordering::equivalent) {
      return is_column_reversed(column_index) ? reverse(element_ordering) : element_ordering;
    }
  }

  return std::weak_ordering::equivalent;
}

// Stores for each row all the information needed for computing the window function.
// This includes the values of the partition_by and order_by columns as well as the argument value,
// which is the value of the argument column (e.g. b for AVG(b)) and NULL_VALUE if no argument exists.
struct RelevantRowInformation {
  std::vector<AllTypeVariant> partition_values;
  std::vector<AllTypeVariant> order_values;
  AllTypeVariant function_argument;
  RowID row_id;

  bool is_peer_of(const RelevantRowInformation& other) const;
};

constexpr uint8_t hash_partition_bits = 8;
constexpr size_t hash_partition_mask = (1u << hash_partition_bits) - 1;
constexpr uint32_t hash_partition_partition_count = 1u << hash_partition_bits;

template <typename T>
using PerHash = std::array<T, hash_partition_partition_count>;

// For each hash_value a task is spawned and the per_hash_function called with
// the values in the corresponding hash_bucket (data[hash_value]) and also the hash_value
// itself (optional) as parameters.
template <typename T>
void spawn_and_wait_per_hash(PerHash<T>& data, auto&& per_hash_function) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(hash_partition_partition_count);
  for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
    tasks.emplace_back(std::make_shared<JobTask>([hash_value, &data, &per_hash_function]() {
      if constexpr (requires { per_hash_function(data[hash_value], hash_value); })
        per_hash_function(data[hash_value], hash_value);
      else
        per_hash_function(data[hash_value]);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
}

template <typename T>
void spawn_and_wait_per_hash(const PerHash<T>& data, auto&& per_hash_function) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.reserve(hash_partition_partition_count);
  for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
    tasks.emplace_back(std::make_shared<JobTask>([hash_value, &data, &per_hash_function]() {
      if constexpr (requires { per_hash_function(data[hash_value], hash_value); })
        per_hash_function(data[hash_value], hash_value);
      else
        per_hash_function(data[hash_value]);
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
}

using HashPartitionedData = PerHash<std::vector<RelevantRowInformation>>;

// Because we might have multiple partitions within the same hash_value, for_each_partition finds all
// partition bounds inside a hash_partition and calls emit_partition_bounds with the computed partition bounds.
void for_each_partition(std::span<const RelevantRowInformation> hash_partition, auto&& emit_partition_bounds) {
  auto partition_start = static_cast<size_t>(0);

  while (partition_start < hash_partition.size()) {
    const auto partition_end = std::distance(
        hash_partition.begin(),
        std::find_if(hash_partition.begin() + static_cast<ssize_t>(partition_start) + 1, hash_partition.end(),
                     [&](const auto& next_element) {
                       return std::is_neq(compare_with_null_equal(hash_partition[partition_start].partition_values,
                                                                  next_element.partition_values));
                     }));
    emit_partition_bounds(partition_start, partition_end);
    partition_start = partition_end;
  }
}

}  // namespace hyrise::window_function_evaluator
