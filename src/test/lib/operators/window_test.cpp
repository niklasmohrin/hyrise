#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/window_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/window_function_evaluator.hpp"
#include "storage/table.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

FrameDescription build_frame(FrameType type = FrameType::Range, uint64_t offset_preceding = 0,
                             bool preceding_unbounded = true, uint64_t offset_following = 0,
                             bool following_unbounded = false) {
  const auto frame_start = FrameBound(offset_preceding, FrameBoundType::Preceding, preceding_unbounded);
  const auto frame_end = FrameBound(offset_following, FrameBoundType::CurrentRow, following_unbounded);
  return FrameDescription(type, frame_start, frame_end);
}

struct WindowOperatorFactory {
 public:
  WindowOperatorFactory(const std::shared_ptr<Table>& table, std::vector<ColumnID> init_partition_by_columns,
                        std::vector<ColumnID> init_order_by_columns, std::vector<SortMode> init_sort_modes)
      : _partition_by_columns{std::move(init_partition_by_columns)},
        _order_by_columns{std::move(init_order_by_columns)},
        _sort_modes{std::move(init_sort_modes)} {
    _static_table_node = StaticTableNode::make(table);
    _table_wrapper = std::make_shared<TableWrapper>(table);
    _table_wrapper->never_clear_output();
    _table_wrapper->execute();

    const auto partition_column_count = _partition_by_columns.size();
    _partition_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(partition_column_count);
    for (auto column_index = size_t{0}; column_index < partition_column_count; ++column_index) {
      _partition_by_expressions[column_index] = lqp_column_(_static_table_node, _partition_by_columns[column_index]);
    }

    const auto order_by_column_count = _order_by_columns.size();
    _order_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>(order_by_column_count);
    for (auto column_index = size_t{0}; column_index < order_by_column_count; ++column_index) {
      _order_by_expressions[column_index] = lqp_column_(_static_table_node, _order_by_columns[column_index]);
    }
  }

  std::shared_ptr<WindowFunctionEvaluator> build_operator(FrameDescription frame_description,
                                                          WindowFunction function_type,
                                                          ColumnID argument_column = INVALID_COLUMN_ID) {
    const auto window = window_(std::move(_partition_by_expressions), std::move(_order_by_expressions),
                                std::move(_sort_modes), std::move(frame_description));
    const auto window_function_expression = [&]() {
      if (window_function_evaluator::is_rank_like(function_type)) {
        return std::make_shared<WindowFunctionExpression>(function_type, nullptr, window);
      }
      const auto column_expression = std::make_shared<LQPColumnExpression>(_static_table_node, argument_column);
      return std::make_shared<WindowFunctionExpression>(function_type, column_expression, window);
    }();

    // If an invalid WindowFunction is used, we set the output_column_name to "Null", to test the handling
    // of invalid WindowFunction types of our operator.
    const auto output_column_name =
        function_type != WindowFunction{-1} ? window_function_expression->as_column_name() : "NULL";
    return std::make_shared<WindowFunctionEvaluator>(_table_wrapper, _partition_by_columns, _order_by_columns,
                                                     argument_column, function_type, window, output_column_name);
  }

  std::shared_ptr<TableWrapper> _table_wrapper;
  std::shared_ptr<StaticTableNode> _static_table_node;
  std::vector<ColumnID> _partition_by_columns;
  std::vector<std::shared_ptr<AbstractExpression>> _partition_by_expressions;
  std::vector<ColumnID> _order_by_columns;
  std::vector<std::shared_ptr<AbstractExpression>> _order_by_expressions;
  std::vector<SortMode> _sort_modes;
};

void test_output(const std::shared_ptr<WindowFunctionEvaluator>& window_function_operator,
                 const std::string& answer_table) {
  const auto* const result_table_path = "resources/test_data/tbl/window_operator/";

  window_function_operator->execute();
  const auto result_table = window_function_operator->get_output();

  const auto expected_result = load_table(result_table_path + answer_table);
  EXPECT_TABLE_EQ_UNORDERED(result_table, expected_result);
}

class OperatorsWindowTest : public BaseTest {
 public:
  static void SetUpTestSuite() {
    _table = load_table("resources/test_data/tbl/window_operator/input_not_unique.tbl", ChunkOffset{2});
    _table_3_columns = load_table("resources/test_data/tbl/window_operator/input_three_columns.tbl", ChunkOffset{2});

    const auto partition_columns = std::vector<ColumnID>{ColumnID{0}};
    const auto order_by_columns = std::vector<ColumnID>{ColumnID{1}};
    const auto sort_modes = std::vector<SortMode>{SortMode::Ascending};
    const auto sort_modes_reverse = std::vector<SortMode>{SortMode::Descending};

    _window_operator_factory =
        std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_columns, sort_modes);
    _window_operator_factory_3_columns =
        std::make_shared<WindowOperatorFactory>(_table_3_columns, partition_columns, order_by_columns, sort_modes);
    _window_operator_factory_reverse =
        std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_columns, sort_modes_reverse);
  }

  static bool operators_equal(const auto& operator_a, const auto& operator_b) {
    return (operator_a->_partition_by_column_ids == operator_b->_partition_by_column_ids &&
            operator_a->_order_by_column_ids == operator_b->_order_by_column_ids &&
            operator_a->_function_argument_column_id == operator_b->_function_argument_column_id &&
            operator_a->_window_function == operator_b->_window_function &&
            operator_a->_window == operator_b->_window &&
            operator_a->_output_column_name == operator_b->_output_column_name);
  }

 protected:
  inline static std::shared_ptr<Table> _table;
  inline static std::shared_ptr<Table> _table_3_columns;
  inline static std::shared_ptr<WindowOperatorFactory> _window_operator_factory;
  inline static std::shared_ptr<WindowOperatorFactory> _window_operator_factory_3_columns;
  inline static std::shared_ptr<WindowOperatorFactory> _window_operator_factory_reverse;
};

TEST_F(OperatorsWindowTest, OperatorName) {
  auto frame = build_frame();
  const auto window_function_operator =
      _window_operator_factory->build_operator(std::move(frame), WindowFunction::RowNumber);
  EXPECT_EQ(window_function_operator->name(), "WindowFunctionEvaluator");
}

TEST_F(OperatorsWindowTest, Description) {
  auto window_function_operator =
      _window_operator_factory_3_columns->build_operator(build_frame(), WindowFunction::Sum, ColumnID{2});
  const auto* const expected_description_without_lqp =
      "WindowFunctionEvaluator\n"
      "SUM(Column #2)\n"
      "PartitionBy: {Column #0}\n"
      "OrderBy: {Column #1}\n"
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";

  EXPECT_EQ(window_function_operator->description(DescriptionMode::MultiLine), expected_description_without_lqp);

  // We set up mocked LQP nodes so that the operator can read the column names for its description.
  const auto window_node = std::make_shared<MockNode>(MockNode::ColumnDefinitions{/* does not matter */});
  window_node->set_left_input(std::make_shared<MockNode>(
      MockNode::ColumnDefinitions{{DataType::Null, "A"}, {DataType::Null, "B"}, {DataType::Null, "C"}}));
  window_function_operator->lqp_node = window_node;

  const auto* const expected_description_with_lqp =
      "WindowFunctionEvaluator\n"
      "SUM(C)\n"
      "PartitionBy: {A}\n"
      "OrderBy: {B}\n"
      "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
  EXPECT_EQ(window_function_operator->description(DescriptionMode::MultiLine), expected_description_with_lqp);

  const auto* const expected_description_single_line =
      "WindowFunctionEvaluator SUM(C) PartitionBy: {A} OrderBy: {B} RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";

  EXPECT_EQ(window_function_operator->description(DescriptionMode::SingleLine), expected_description_single_line);
}

TEST_F(OperatorsWindowTest, DeepCopy) {
  const auto frame = build_frame();
  const auto original_operator = _window_operator_factory->build_operator(frame, WindowFunction::Rank);
  const auto copied_operator = std::dynamic_pointer_cast<WindowFunctionEvaluator>(original_operator->deep_copy());
  EXPECT_TRUE(operators_equal(original_operator, copied_operator));
}

TEST_F(OperatorsWindowTest, ExactlyOneOrderByForRange) {
  const auto partition_columns = std::vector<ColumnID>{ColumnID{0}};
  const auto order_by_columns = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  const auto sort_modes = std::vector<SortMode>(2, SortMode::Ascending);

  const auto frame = build_frame();
  const auto invalid_operator_factory =
      std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_columns, sort_modes);
  const auto window_function_operator_invalid =
      invalid_operator_factory->build_operator(frame, WindowFunction::Sum, ColumnID{1});
  // For non-rank-like window functions (e.g. sum/avg/etc.),
  // range mode frames are only allowed when there is at most one order-by column.
  EXPECT_THROW(window_function_operator_invalid->execute(), std::logic_error);

  const auto order_by_single_column = std::vector<ColumnID>{ColumnID{1}};
  const auto sort_single_mode = std::vector<SortMode>{SortMode::Ascending};
  const auto valid_operator_factory =
      std::make_shared<WindowOperatorFactory>(_table, partition_columns, order_by_single_column, sort_single_mode);
  const auto window_function_operator_valid =
      valid_operator_factory->build_operator(frame, WindowFunction::Sum, ColumnID{1});

  EXPECT_NO_THROW(window_function_operator_valid->execute());
}

TEST_F(OperatorsWindowTest, NonRankLikeHasArgument) {
  const auto frame = build_frame(FrameType::Rows);
  // Non-rank-like window functions need an argument column expression.
  EXPECT_THROW(_window_operator_factory->build_operator(frame, WindowFunction::Sum), std::logic_error);
  EXPECT_NO_THROW(_window_operator_factory->build_operator(frame, WindowFunction::Sum, ColumnID{0}));
}

TEST_F(OperatorsWindowTest, InvalidWindowFunction) {
  const auto frame = build_frame();
  const auto unknown_window_function_operator =
      _window_operator_factory->build_operator(frame, WindowFunction{-1}, ColumnID{1});
  EXPECT_THROW(unknown_window_function_operator->execute(), std::logic_error);

  const auto percent_rank_window_function_operator =
      _window_operator_factory->build_operator(frame, WindowFunction::PercentRank, ColumnID{1});
  // PercentRank is currently not supported.
  EXPECT_THROW(percent_rank_window_function_operator->execute(), std::logic_error);
}

TEST_F(OperatorsWindowTest, ReverseOrdering) {
  const auto frame = build_frame();
  const auto window_function_operator = _window_operator_factory_reverse->build_operator(frame, WindowFunction::Rank);
  test_output(window_function_operator, "rank_reverse.tbl");
}

TEST_F(OperatorsWindowTest, Rank) {
  const auto frame = build_frame();
  const auto rank_like_operator = _window_operator_factory->build_operator(frame, WindowFunction::Rank);
  EXPECT_NO_THROW(rank_like_operator->execute());

  const auto invalid_frame = build_frame(FrameType::Range, 1, false, 0, false);
  const auto invalid_rank_like_operator = _window_operator_factory->build_operator(invalid_frame, WindowFunction::Rank);
  // Rank-like window functions only work with a frame: BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
  EXPECT_THROW(invalid_rank_like_operator->execute(), std::logic_error);
}

TEST_F(OperatorsWindowTest, DenseRank) {
  const auto frame = build_frame();
  const auto window_function_operator = _window_operator_factory->build_operator(frame, WindowFunction::DenseRank);
  test_output(window_function_operator, "dense_rank.tbl");
}

TEST_F(OperatorsWindowTest, RowNumber) {
  const auto frame = build_frame();
  const auto window_function_operator = _window_operator_factory->build_operator(frame, WindowFunction::RowNumber);
  test_output(window_function_operator, "row_number.tbl");
}

TEST_F(OperatorsWindowTest, Sum) {
  const auto frame = build_frame(FrameType::Rows);
  const auto sum_window_operator = _window_operator_factory->build_operator(frame, WindowFunction::Sum, ColumnID{1});
  test_output(sum_window_operator, "sum.tbl");

  const auto range_frame = build_frame();
  const auto range_sum_window_operator =
      _window_operator_factory->build_operator(range_frame, WindowFunction::Sum, ColumnID{1});
  test_output(range_sum_window_operator, "prefix_sum.tbl");
}

TEST_F(OperatorsWindowTest, AVG) {
  const auto frame = build_frame(FrameType::Rows);
  const auto avg_window_operator = _window_operator_factory->build_operator(frame, WindowFunction::Avg, ColumnID{1});
  test_output(avg_window_operator, "avg.tbl");

  const auto range_frame = build_frame(FrameType::Range, 3, false, 0, false);
  const auto range_avg_window_operator =
      _window_operator_factory->build_operator(range_frame, WindowFunction::Avg, ColumnID{1});
  test_output(range_avg_window_operator, "range_avg.tbl");
}

TEST_F(OperatorsWindowTest, Min) {
  const auto frame = build_frame(FrameType::Rows);
  const auto min_window_operator = _window_operator_factory->build_operator(frame, WindowFunction::Min, ColumnID{1});
  test_output(min_window_operator, "min.tbl");

  const auto range_frame = build_frame(FrameType::Range, 3, false, 0, false);
  const auto range_min_window_operator =
      _window_operator_factory->build_operator(range_frame, WindowFunction::Min, ColumnID{1});
  test_output(range_min_window_operator, "range_min.tbl");
}

TEST_F(OperatorsWindowTest, Max) {
  const auto frame = build_frame(FrameType::Rows);
  const auto max_window_operator = _window_operator_factory->build_operator(frame, WindowFunction::Max, ColumnID{1});
  test_output(max_window_operator, "max.tbl");

  const auto range_frame = build_frame(FrameType::Range, 0, false, 1, false);
  const auto range_max_window_operator =
      _window_operator_factory->build_operator(range_frame, WindowFunction::Max, ColumnID{1});
  test_output(range_max_window_operator, "range_max.tbl");
}

}  // namespace hyrise
