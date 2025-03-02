#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include <utility>

#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/segment_tree.hpp"
#include "utils/timer.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise::window_function_evaluator {

WindowFunctionEvaluator::WindowFunctionEvaluator(const std::shared_ptr<const AbstractOperator>& input_operator,
                                                 std::vector<ColumnID> init_partition_by_column_ids,
                                                 std::vector<ColumnID> init_order_by_column_ids,
                                                 ColumnID init_function_argument_column_id,
                                                 WindowFunction init_window_function,
                                                 std::shared_ptr<WindowExpression> init_window,
                                                 std::string init_output_column_name)
    : AbstractReadOnlyOperator(OperatorType::WindowFunction, input_operator, nullptr,
                               std::make_unique<PerformanceData>()),
      _partition_by_column_ids(std::move(init_partition_by_column_ids)),
      _order_by_column_ids(std::move(init_order_by_column_ids)),
      _function_argument_column_id(init_function_argument_column_id),
      _window_function(init_window_function),
      _window(std::move(init_window)),
      _output_column_name(std::move(init_output_column_name)) {
  Assert(_function_argument_column_id != INVALID_COLUMN_ID || is_rank_like(_window_function),
         "Could not extract window function argument, although it was not rank-like.");
}

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

std::string WindowFunctionEvaluator::description(const DescriptionMode description_mode) const {
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  std::stringstream desc;

  const auto output_column = [&](ColumnID column_id) {
    if (lqp_node) {
      desc << lqp_node->left_input()->output_expressions()[column_id]->as_column_name();
    } else {
      desc << "Column #" + std::to_string(column_id);
    }
  };

  const auto output_columns = [&](const std::span<const ColumnID> column_ids) {
    if (column_ids.empty()) {
      return;
    }
    output_column(column_ids.front());
    for (const auto& column_id : column_ids.subspan(1)) {
      desc << ", ";
      output_column(column_id);
    }
  };

  desc << AbstractOperator::description(description_mode) << separator;

  desc << window_function_to_string.left.at(_window_function) << '(';
  if (_function_argument_column_id != INVALID_COLUMN_ID) {
    output_column(_function_argument_column_id);
  }
  desc << ')' << separator;

  desc << "PartitionBy: {";
  output_columns(_partition_by_column_ids);
  desc << "}" << separator;

  desc << "OrderBy: {";
  output_columns(_order_by_column_ids);
  desc << "}" << separator;

  desc << frame_description();

  return desc.str();
}

const FrameDescription& WindowFunctionEvaluator::frame_description() const {
  return _window->frame_description;
}

std::shared_ptr<AbstractOperator> WindowFunctionEvaluator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    [[maybe_unused]] const std::shared_ptr<AbstractOperator>& copied_right_input,
    [[maybe_unused]] std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<WindowFunctionEvaluator>(copied_left_input, _partition_by_column_ids, _order_by_column_ids,
                                                   _function_argument_column_id, _window_function, _window,
                                                   _output_column_name);
}

template <typename InputColumnType, WindowFunction window_function>
std::shared_ptr<const Table> WindowFunctionEvaluator::_templated_on_execute() {
  if (!is_rank_like(window_function) && frame_description().type == FrameType::Range) {
    // The SQL standard only allows for at most one order-by column when using range mode.
    // If no order-by column is provided all tuples are treated as peers.
    Assert(_order_by_column_ids.size() <= 1,
           "For non-rank-like window functions, range mode frames are only allowed when there is at most one order-by "
           "expression.");
  }

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  auto& window_performance_data = dynamic_cast<PerformanceData&>(*performance_data);

  auto timer = Timer{};

  const auto run_and_cleanup = [&]() {
    auto buckets = materialize_into_buckets();
    window_performance_data.set_step_runtime(OperatorSteps::MaterializeIntoBuckets, timer.lap());

    partition_and_order(buckets);
    window_performance_data.set_step_runtime(OperatorSteps::PartitionAndOrder, timer.lap());

    using OutputColumnType = typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::OutputColumnType;
    using IsNull = bool;

    // The segment_data_for_output_column stores the computed aggregates of the window function
    // and an isNull value for each chunk and row.
    auto segment_data_for_output_column =
        std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<IsNull>>>(chunk_count);

    // For each chunk we resize the pmr_vector for the aggregate values and the isNull values to the chunk size.
    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      const auto output_length = input_table->get_chunk(chunk_id)->size();
      segment_data_for_output_column[chunk_id].first.resize(output_length);
      segment_data_for_output_column[chunk_id].second.resize(output_length);
    }

    // The emit_computed_value function is called by either compute_window_function_one_pass and
    // compute_window_function_segment_tree for each RowID and computed value. The computed value
    // is then moved to the correct position in segment_data_for_output_column and the IsNull value is set accordingly.
    const auto emit_computed_value = [&](RowID row_id, std::optional<OutputColumnType> computed_value) {
      if (computed_value) {
        segment_data_for_output_column[row_id.chunk_id].first[row_id.chunk_offset] = std::move(*computed_value);
        // We don't need to set segment_data_for_output_column[row_id.chunk_id].second[row_id.chunk_offset] = false,
        // because it is default-constructed to false.
      } else {
        segment_data_for_output_column[row_id.chunk_id].second[row_id.chunk_offset] = true;
      }
    };

    const auto computation_strategy = choose_computation_strategy<InputColumnType, window_function>();
    window_performance_data.computation_strategy = computation_strategy;

    switch (computation_strategy) {
      case ComputationStrategy::OnePass:
        if constexpr (SupportsOnePass<InputColumnType, window_function>) {
          compute_window_function_one_pass<InputColumnType, window_function>(buckets, emit_computed_value);
        } else {
          Fail("Chose ComputationStrategy::OnePass, although it is not supported!");
        }
        break;
      case ComputationStrategy::SegmentTree:
        if constexpr (SupportsSegmentTree<InputColumnType, window_function>) {
          compute_window_function_segment_tree<InputColumnType, window_function>(buckets, emit_computed_value);
        } else {
          Fail("Chose ComputationStrategy::SegmentTree, although it is not supported!");
        }
        break;
    }
    window_performance_data.set_step_runtime(OperatorSteps::Compute, timer.lap());

    const auto annotated_table = annotate_input_table(std::move(segment_data_for_output_column));
    window_performance_data.set_step_runtime(OperatorSteps::Annotate, timer.lap());

    return annotated_table;
  };

  const auto result = run_and_cleanup();
  window_performance_data.set_step_runtime(OperatorSteps::Cleanup, timer.lap());
  return result;
}

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  auto result = std::shared_ptr<const Table>{};

  if (!magic_enum::enum_contains(_window_function)) {
    Fail("Unknown window function: " + std::to_string(magic_enum::enum_underlying(_window_function)) + ".");
  }

  magic_enum::enum_switch(
      [&](const auto window_function) {
        if constexpr (!is_supported_window_function(window_function)) {
          Fail("Unsupported window function: " + window_function_to_string.left.at(window_function) + ".");
        } else {
          if constexpr (!has_argument(window_function)) {
            result = _templated_on_execute<NullValue, window_function>();
          } else {
            const auto input_data_type = left_input_table()->column_data_type(_function_argument_column_id);

            resolve_data_type(input_data_type, [&](const auto const_input_data_type) {
              using InputColumnType = typename decltype(const_input_data_type)::type;

              if constexpr (SupportsAnyStrategy<InputColumnType, window_function>) {
                result = _templated_on_execute<InputColumnType, window_function>();
              } else {
                Fail("Unsupported input column type " + data_type_to_string.left.at(input_data_type) +
                     " for window function " + window_function_to_string.left.at(window_function) + ".");
              }
            });
          }
        }
      },
      _window_function);

  return result;
}

namespace {
template <typename T>
T clamped_add(T lhs, T rhs, T max) {
  return std::min(lhs + rhs, max);
}

template <typename T>
T clamped_sub(T lhs, T rhs, T min) {
  using U = std::make_signed_t<T>;
  return static_cast<T>(std::max(static_cast<U>(static_cast<U>(lhs) - static_cast<U>(rhs)), static_cast<U>(min)));
}

}  // namespace

template <typename InputColumnType, WindowFunction window_function>
ComputationStrategy WindowFunctionEvaluator::choose_computation_strategy() const {
  const auto& frame = frame_description();
  const auto is_prefix_frame = frame.start.unbounded && frame.start.type == FrameBoundType::Preceding &&
                               !frame.end.unbounded && frame.end.type == FrameBoundType::CurrentRow;
  Assert(is_prefix_frame || !is_rank_like(window_function), "Invalid frame for rank-like window function.");

  if (is_prefix_frame && SupportsOnePass<InputColumnType, window_function> &&
      (is_rank_like(window_function) || frame.type == FrameType::Rows)) {
    return ComputationStrategy::OnePass;
  }

  if (SupportsSegmentTree<InputColumnType, window_function>) {
    return ComputationStrategy::SegmentTree;
  }

  Fail("Could not determine appropriate computation strategy for window function " +
       window_function_to_string.left.at(window_function) + ".");
}

bool WindowFunctionEvaluator::is_output_nullable() const {
  return !is_rank_like(_window_function) && left_input_table()->column_is_nullable(_function_argument_column_id);
}

namespace {

Buckets collect_chunk_into_buckets(ChunkID chunk_id, const Chunk& chunk, std::span<const ColumnID> partition_column_ids,
                                   std::span<const ColumnID> order_column_ids, ColumnID function_argument_column_id) {
  const auto chunk_size = chunk.size();

  const auto collect_values_of_columns = [&chunk, chunk_size](std::span<const ColumnID> column_ids) {
    const auto column_count = column_ids.size();
    auto values = std::vector(chunk_size, std::vector(column_count, NULL_VALUE));
    for (auto column_id_index = 0u; column_id_index < column_count; ++column_id_index) {
      const auto segment = chunk.get_segment(column_ids[column_id_index]);
      segment_iterate(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          values[position.chunk_offset()][column_id_index] = position.value();
        }
      });
    }
    return values;
  };

  auto partition_values = collect_values_of_columns(partition_column_ids);
  auto order_values = collect_values_of_columns(order_column_ids);

  auto function_argument_values = std::vector<AllTypeVariant>(chunk_size, NULL_VALUE);
  if (function_argument_column_id != INVALID_COLUMN_ID) {
    segment_iterate(*chunk.get_segment(function_argument_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        function_argument_values[position.chunk_offset()] = position.value();
      }
    });
  }

  auto result = Buckets{};

  for (auto chunk_offset = ChunkOffset(0); chunk_offset < chunk_size; ++chunk_offset) {
    auto row_info = RelevantRowInformation{
        .partition_values = std::move(partition_values[chunk_offset]),
        .order_values = std::move(order_values[chunk_offset]),
        .function_argument = std::move(function_argument_values[chunk_offset]),
        .row_id = RowID(chunk_id, chunk_offset),
    };
    const auto bucket_index =
        boost::hash_range(row_info.partition_values.begin(), row_info.partition_values.end()) & bucket_mask;
    result[bucket_index].push_back(std::move(row_info));
  }

  return result;
}

}  // namespace

Buckets WindowFunctionEvaluator::materialize_into_buckets() const {
  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);
  auto chunk_buckets = std::vector<Buckets>(chunk_count);

  // Parallel for each chunk, materialize and partition into chunk_buckets.
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    tasks[chunk_id] = std::make_shared<JobTask>([chunk_id, &input_table, &chunk_buckets, this]() {
      const auto chunk = input_table->get_chunk(chunk_id);
      // Going forward, we will not check the assertion again, since the output table of the previous operator should
      // not change during execution of the current operator. Similarly, while there could generally be tables with
      // deleted chunks, this should not happen with the tables in the operator pipeline as `GetTable` should remove any
      // such chunks already.
      Assert(chunk, "Got nullptr from `get_chunk` in `materialize_into_buckets`.");
      chunk_buckets[chunk_id] = collect_chunk_into_buckets(chunk_id, *chunk, _partition_by_column_ids,
                                                           _order_by_column_ids, _function_argument_column_id);
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  // Next, the buckets need to be merged. We want to move the data in the buckets for each chunk in parallel, so we
  // first compute the indices at which each task can safely write without interfering with another task. Hence, for
  // each chunk and hash value, we compute the prefix sum of the chunk bucket sizes and store it in starting_indices.
  // This means that the tasks move the data as if the chunk buckets would have been concatenated by a single task. In
  // addition, starting_indices has one more element, starting_indices[chunk_count], which contains the complete sums of
  // the sizes and therefore equals the total size of the corresponding output bucket.
  auto starting_indices = std::vector<PerHash<ssize_t>>(chunk_count + 1);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto& my_start = starting_indices[chunk_id];
    auto& next_start = starting_indices[chunk_id + 1];

    for (auto hash_value = 0u; hash_value < bucket_count; ++hash_value) {
      next_start[hash_value] = my_start[hash_value] + static_cast<ssize_t>(chunk_buckets[chunk_id][hash_value].size());
    }
  }

  // Note that the vector may not resize during writing, because multiple tasks are writing to it in parallel and at
  // indices spread throughout it. Hence, it is resized here before the writing starts.
  auto output_buckets = Buckets{};
  for (auto hash_value = 0u; hash_value < bucket_count; ++hash_value) {
    const auto bucket_size = starting_indices[chunk_count][hash_value];
    output_buckets[hash_value].resize(bucket_size);
  }

  // Finally we can move the chunk_buckets to their correct position in output_buckets.
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    tasks[chunk_id] = std::make_shared<JobTask>([chunk_id, &chunk_buckets, &output_buckets, &starting_indices]() {
      for (auto hash_value = 0u; hash_value < bucket_count; ++hash_value) {
        auto& my_values = chunk_buckets[chunk_id][hash_value];
        std::ranges::move(my_values, output_buckets[hash_value].begin() + starting_indices[chunk_id][hash_value]);
      }
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return output_buckets;
}

void WindowFunctionEvaluator::partition_and_order(Buckets& buckets) const {
  const auto& sort_modes = _window->sort_modes;
  const auto is_column_reversed = [&sort_modes](const auto column_index) {
    return sort_modes[column_index] == SortMode::Descending;
  };

  const auto comparator = [&is_column_reversed](const RelevantRowInformation& lhs, const RelevantRowInformation& rhs) {
    const auto comp_result = compare_with_null_equal(lhs.partition_values, rhs.partition_values);
    if (std::is_neq(comp_result)) {
      return std::is_lt(comp_result);
    }
    return std::is_lt(compare_with_null_equal(lhs.order_values, rhs.order_values, is_column_reversed));
  };

  spawn_and_wait_per_hash(buckets, [&comparator](auto& bucket) {
    // TODO(anyone): Change this to parallel merge sort implementation from PR #2605 when it is merged
    std::ranges::stable_sort(bucket, comparator);
  });
}

template <typename InputColumnType, WindowFunction window_function>
  requires SupportsOnePass<InputColumnType, window_function>
void WindowFunctionEvaluator::compute_window_function_one_pass(const Buckets& buckets,
                                                               auto&& emit_computed_value) const {
  spawn_and_wait_per_hash(buckets, [&emit_computed_value](const auto& bucket) {
    using Traits = WindowFunctionEvaluatorTraits<InputColumnType, window_function>;
    using Impl = typename Traits::OnePassImpl;
    using State = typename Impl::State;
    auto state = State{};

    const RelevantRowInformation* previous_row = nullptr;

    for (const auto& row : bucket) {
      if (previous_row && std::is_eq(compare_with_null_equal(previous_row->partition_values, row.partition_values))) {
        Impl::update_state(state, row);
      } else {
        state = Impl::initial_state(row);
      }
      emit_computed_value(row.row_id, Impl::current_value(state));
      previous_row = &row;
    }
  });
}

namespace {

// Sentinel type to indicate that Range mode is used without an order-by column.
struct NoOrderByColumn {};

// WindowBoundCalculator should only be used with the partial specializations frame_type=FrameType::Rows and
// frame_type=FrameType::Range. If none of these types is selected,
// this implementation will be chosen and calculate_window_bounds will always Fail.
template <FrameType frame_type, typename OrderByColumnType>
struct WindowBoundCalculator {
  static QueryRange calculate_window_bounds([[maybe_unused]] std::span<const RelevantRowInformation> partition,
                                            [[maybe_unused]] uint64_t tuple_index,
                                            [[maybe_unused]] const FrameDescription& frame) {
    auto error_stream =
        std::ostringstream("Unsupported frame type and order-by column type combination: ", std::ios_base::ate);
    error_stream << [&]() {
      using std::literals::string_view_literals::operator""sv;

      switch (frame_type) {
        case FrameType::Rows:
          return "Rows"sv;
        case FrameType::Range:
          return "Range"sv;
        case FrameType::Groups:
          return "Groups"sv;
      }
      Fail("Invalid FrameType.");
    }();
    error_stream << " ";
    error_stream << data_type_to_string.left.at(data_type_from_type<OrderByColumnType>());
    Fail(error_stream.str());
  }
};

template <typename OrderByColumnType>
struct WindowBoundCalculator<FrameType::Rows, OrderByColumnType> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition, uint64_t tuple_index,
                                            const FrameDescription& frame) {
    const auto start = frame.start.unbounded ? 0 : clamped_sub<uint64_t>(tuple_index, frame.start.offset, 0);
    const auto end = frame.end.unbounded ? partition.size()
                                         : clamped_add<uint64_t>(tuple_index, frame.end.offset + 1, partition.size());
    return {start, end};
  }
};

template <>
struct WindowBoundCalculator<FrameType::Range, NoOrderByColumn> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition,
                                            [[maybe_unused]] uint64_t tuple_index,
                                            [[maybe_unused]] const FrameDescription& frame) {
    return {0, partition.size()};
  }
};

template <typename OrderByColumnType>
  requires std::is_integral_v<OrderByColumnType>
struct WindowBoundCalculator<FrameType::Range, OrderByColumnType> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition, uint64_t tuple_index,
                                            const FrameDescription& frame) {
    const auto current_value = as_optional<OrderByColumnType>(partition[tuple_index].order_values[0]);

    // We sort null-first
    const auto end_of_null_peer_group_it =
        std::ranges::partition_point(partition, [&](const auto& row) { return variant_is_null(row.order_values[0]); });
    const auto end_of_null_peer_group = std::distance(partition.begin(), end_of_null_peer_group_it);
    const auto non_null_values = partition.subspan(end_of_null_peer_group);

    const auto start = [&]() -> uint64_t {
      if (frame.start.unbounded) {
        return 0;
      }
      if (!current_value) {
        return 0;
      }
      const auto it = std::ranges::partition_point(non_null_values, [&](const auto& row) {
        return std::cmp_less(get<OrderByColumnType>(row.order_values[0]) + frame.start.offset, *current_value);
      });
      return end_of_null_peer_group + std::distance(non_null_values.begin(), it);
    }();

    const auto end = [&]() -> uint64_t {
      if (frame.end.unbounded) {
        return partition.size();
      }
      if (!current_value) {
        return end_of_null_peer_group;
      }
      const auto it = std::ranges::partition_point(non_null_values, [&](const auto& row) {
        return std::cmp_less_equal(get<OrderByColumnType>(row.order_values[0]), *current_value + frame.end.offset);
      });
      return end_of_null_peer_group + std::distance(non_null_values.begin(), it);
    }();

    return {start, end};
  }
};

template <typename InputColumnType, WindowFunction window_function, FrameType frame_type, typename OrderByColumnType>
  requires SupportsSegmentTree<InputColumnType, window_function>
void templated_compute_window_function_segment_tree(const Buckets& buckets, const FrameDescription& frame,
                                                    auto&& emit_computed_value) {
  spawn_and_wait_per_hash(buckets, [&emit_computed_value, &frame](const auto& bucket) {
    for_each_partition(bucket, [&](uint64_t partition_start, uint64_t partition_end) {
      const auto partition = std::span(bucket.begin() + partition_start, partition_end - partition_start);

      using Traits = WindowFunctionEvaluatorTraits<InputColumnType, window_function>;
      using Impl = typename Traits::NullableSegmentTreeImpl;
      using TreeNode = typename Impl::TreeNode;

      const auto partition_size = partition.size();

      auto leaf_values = std::vector<TreeNode>(partition_size);
      std::ranges::transform(partition, leaf_values.begin(), [](const auto& row) {
        return Impl::node_from_value(as_optional<InputColumnType>(row.function_argument));
      });

      const auto combine = [](auto lhs, auto rhs) { return Impl::combine(std::move(lhs), std::move(rhs)); };
      const auto make_neutral_element = []() { return Impl::neutral_element; };
      const auto segment_tree = SegmentTree<TreeNode, decltype(combine), decltype(make_neutral_element)>(
          leaf_values, combine, make_neutral_element);

      for (auto tuple_index = 0u; tuple_index < partition_size; ++tuple_index) {
        using BoundCalculator = WindowBoundCalculator<frame_type, OrderByColumnType>;
        const auto query_range = BoundCalculator::calculate_window_bounds(partition, tuple_index, frame);
        emit_computed_value(partition[tuple_index].row_id,
                            Impl::transform_query(segment_tree.range_query(query_range)));
      }
    });
  });
}

}  // namespace

template <typename InputColumnType, WindowFunction window_function>
  requires SupportsSegmentTree<InputColumnType, window_function>
void WindowFunctionEvaluator::compute_window_function_segment_tree(const Buckets& buckets,
                                                                   auto&& emit_computed_value) const {
  const auto& frame = frame_description();
  switch (frame.type) {
    case FrameType::Rows:
      templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Rows, void>(
          buckets, frame, emit_computed_value);
      break;
    case FrameType::Range:
      if (_order_by_column_ids.empty()) {
        templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Range,
                                                       NoOrderByColumn>(buckets, frame, emit_computed_value);
      } else {
        resolve_data_type(left_input_table()->column_data_type(_order_by_column_ids[0]), [&](auto data_type) {
          using OrderByColumnType = typename decltype(data_type)::type;
          templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Range,
                                                         OrderByColumnType>(buckets, frame, emit_computed_value);
        });
      }
      break;
    case FrameType::Groups:
      Fail("Unsupported frame type: Groups.");
  }
}

template <typename OutputColumnType>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<bool>>> segment_data_for_output_column) const {
  const auto input_table = left_input_table();

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  const auto new_column_definition =
      TableColumnDefinition(_output_column_name, data_type_from_type<OutputColumnType>(), is_output_nullable());

  // Create value segments for our output column.
  auto value_segments_for_new_column = std::vector<std::shared_ptr<AbstractSegment>>();
  value_segments_for_new_column.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    if (is_output_nullable()) {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<OutputColumnType>>(std::move(segment_data_for_output_column[chunk_id].first),
                                                           std::move(segment_data_for_output_column[chunk_id].second)));
    } else {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<OutputColumnType>>(std::move(segment_data_for_output_column[chunk_id].first)));
    }
  }

  auto outputted_segments_for_new_column = std::vector<std::shared_ptr<AbstractSegment>>{};
  if (input_table->type() == TableType::Data) {
    // If our input table is of TableType::Data, use the value segments created above for our output table.
    outputted_segments_for_new_column = std::move(value_segments_for_new_column);
  } else {
    // If our input table is of TableType::Reference, create an extra table with the value segments and use reference
    // segments to this table in our output.
    auto chunks_for_referenced_table = std::vector<std::shared_ptr<Chunk>>{};
    chunks_for_referenced_table.reserve(chunk_count);
    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      chunks_for_referenced_table.emplace_back(
          std::make_shared<Chunk>(Segments({std::move(value_segments_for_new_column[chunk_id])})));
    }
    auto referenced_table = std::make_shared<Table>(TableColumnDefinitions({new_column_definition}), TableType::Data,
                                                    std::move(chunks_for_referenced_table));

    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      const auto pos_list = std::make_shared<EntireChunkPosList>(chunk_id, input_table->get_chunk(chunk_id)->size());
      outputted_segments_for_new_column.emplace_back(
          std::make_shared<ReferenceSegment>(referenced_table, ColumnID{0}, pos_list));
    }
  }

  // Create output table reusing the segments from input and the newly created segments for the output column.
  auto output_column_definitions = input_table->column_definitions();
  output_column_definitions.push_back(new_column_definition);
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();
  output_chunks.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    auto output_segments = Segments();
    for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
      output_segments.emplace_back(chunk->get_segment(column_id));
    }
    output_segments.push_back(std::move(outputted_segments_for_new_column[chunk_id]));

    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }

  return std::make_shared<Table>(output_column_definitions, input_table->type(), std::move(output_chunks));
}

std::ostream& operator<<(std::ostream& stream, const ComputationStrategy computation_strategy) {
  return stream << computation_strategy_to_string.left.at(computation_strategy);
}

void WindowFunctionEvaluator::PerformanceData::output_to_stream(std::ostream& stream,
                                                                DescriptionMode description_mode) const {
  OperatorPerformanceData<OperatorSteps>::output_to_stream(stream, description_mode);

  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  stream << separator << "Computation strategy: " << computation_strategy << ".";
}

}  // namespace hyrise::window_function_evaluator
