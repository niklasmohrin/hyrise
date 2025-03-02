#include "expression_utils.hpp"

#include <algorithm>
#include <queue>
#include <sstream>

#include "expression_functional.hpp"
#include "logical_expression.hpp"
#include "lossy_cast.hpp"
#include "lqp_column_expression.hpp"
#include "lqp_subquery_expression.hpp"
#include "operators/abstract_operator.hpp"
#include "pqp_subquery_expression.hpp"
#include "value_expression.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return std::equal(expressions_a.begin(), expressions_a.end(), expressions_b.begin(), expressions_b.end(),
                    [&](const auto& expression_a, const auto& expression_b) { return *expression_a == *expression_b; });
}

bool expressions_equal_to_expressions_in_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right, const LQPNodeMapping& node_mapping) {
  const auto expressions_left_count = expressions_left.size();
  if (expressions_left_count != expressions_right.size()) {
    return false;
  }

  for (auto expression_idx = size_t{0}; expression_idx < expressions_left_count; ++expression_idx) {
    const auto& expression_left = *expressions_left[expression_idx];
    const auto& expression_right = *expressions_right[expression_idx];

    if (!expression_equal_to_expression_in_different_lqp(expression_left, expression_right, node_mapping)) {
      return false;
    }
  }

  return true;
}

bool expression_equal_to_expression_in_different_lqp(const AbstractExpression& expression_left,
                                                     const AbstractExpression& expression_right,
                                                     const LQPNodeMapping& node_mapping) {
  /**
   * Compare expression_left to expression_right by creating a deep copy of expression_left and adapting it to the LQP
   * of expression_right, then perform a normal comparison of two expressions in the same LQP.
   */

  auto copied_expression_left = expression_left.deep_copy();
  expression_adapt_to_different_lqp(copied_expression_left, node_mapping);
  return *copied_expression_left == expression_right;
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>> copied_ops;
  return expressions_deep_copy(expressions, copied_ops);
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_deep_copy(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression->deep_copy(copied_ops));
  }
  return copied_expressions;
}

void expression_deep_replace(std::shared_ptr<AbstractExpression>& expression,
                             const ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>& mapping) {
  visit_expression(expression, [&](auto& sub_expression) {
    const auto replacement_iter = mapping.find(sub_expression);
    if (replacement_iter != mapping.end()) {
      sub_expression = replacement_iter->second;
      return ExpressionVisitation::DoNotVisitArguments;
    }

    return ExpressionVisitation::VisitArguments;
  });
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LQPNodeMapping& node_mapping) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());

  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression_copy_and_adapt_to_different_lqp(*expression, node_mapping));
  }

  return copied_expressions;
}

std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(const AbstractExpression& expression,
                                                                               const LQPNodeMapping& node_mapping) {
  auto copied_expression = expression.deep_copy();
  expression_adapt_to_different_lqp(copied_expression, node_mapping);
  return copied_expression;
}

void expression_adapt_to_different_lqp(std::shared_ptr<AbstractExpression>& expression,
                                       const LQPNodeMapping& node_mapping) {
  visit_expression(expression, [&](auto& expression_ptr) {
    if (expression_ptr->type != ExpressionType::LQPColumn) {
      return ExpressionVisitation::VisitArguments;
    }

    const auto lqp_column_expression_ptr = std::dynamic_pointer_cast<LQPColumnExpression>(expression_ptr);
    Assert(lqp_column_expression_ptr, "Asked to adapt expression in LQP, but encountered non-LQP ColumnExpression");

    expression_ptr = expression_adapt_to_different_lqp(*lqp_column_expression_ptr, node_mapping);

    return ExpressionVisitation::DoNotVisitArguments;
  });
}

std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(const LQPColumnExpression& lqp_column_expression,
                                                                       const LQPNodeMapping& node_mapping) {
  const auto node = lqp_column_expression.original_node.lock();
  Assert(node, "LQPColumnExpression is expired");
  const auto node_mapping_iter = node_mapping.find(node);
  Assert(node_mapping_iter != node_mapping.end(),
         "Couldn't find referenced node (" + node->description() + ") in NodeMapping");

  return std::make_shared<LQPColumnExpression>(node_mapping_iter->second, lqp_column_expression.original_column_id);
}

std::string expression_descriptions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                    const AbstractExpression::DescriptionMode mode) {
  std::stringstream stream;

  if (!expressions.empty()) {
    stream << expressions.front()->description(mode);
  }

  for (auto expression_idx = size_t{1}; expression_idx < expressions.size(); ++expression_idx) {
    stream << ", " << expressions[expression_idx]->description(mode);
  }

  return stream.str();
}

DataType expression_common_type(const DataType lhs, const DataType rhs) {
  Assert(lhs != DataType::Null || rhs != DataType::Null, "Can't deduce common type if both sides are NULL");
  Assert((lhs == DataType::String) == (rhs == DataType::String), "Strings only compatible with strings");

  // Long+NULL -> Long; NULL+Long -> Long
  if (lhs == DataType::Null) {
    return rhs;
  }

  if (rhs == DataType::Null) {
    return lhs;
  }

  if (lhs == DataType::String) {
    return DataType::String;
  }

  if (lhs == DataType::Double || rhs == DataType::Double) {
    return DataType::Double;
  }

  if (lhs == DataType::Long) {
    return is_floating_point_data_type(rhs) ? DataType::Double : DataType::Long;
  }

  if (rhs == DataType::Long) {
    return is_floating_point_data_type(lhs) ? DataType::Double : DataType::Long;
  }

  if (lhs == DataType::Float || rhs == DataType::Float) {
    return DataType::Float;
  }

  return DataType::Int;
}

bool expression_evaluable_on_lqp(const std::shared_ptr<AbstractExpression>& expression, const AbstractLQPNode& lqp) {
  auto evaluable = true;

  visit_expression(expression, [&](const auto& sub_expression) {
    if (lqp.find_column_id(*sub_expression)) {
      return ExpressionVisitation::DoNotVisitArguments;
    }

    if (WindowFunctionExpression::is_count_star(*sub_expression)) {
      // COUNT(*) needs special treatment. Because its argument is the invalid column id, it is not part of any node's
      // output_expressions. Check if sub_expression is COUNT(*) - if yes, ignore the INVALID_COLUMN_ID and verify that
      // its original_node is part of lqp.
      const auto& aggregate_expression = static_cast<const WindowFunctionExpression&>(*sub_expression);
      const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(*aggregate_expression.argument());
      const auto& original_node = lqp_column_expression.original_node.lock();
      Assert(original_node, "LQPColumnExpression is expired, LQP is invalid");

      // Now check if lqp contains that original_node
      evaluable = false;
      visit_lqp(lqp.shared_from_this(), [&](const auto& sub_lqp) {
        if (sub_lqp == original_node) {
          evaluable = true;
        }
        return LQPVisitation::VisitInputs;
      });

      return ExpressionVisitation::DoNotVisitArguments;
    }

    if (sub_expression->type == ExpressionType::LQPColumn) {
      evaluable = false;
    }

    return ExpressionVisitation::VisitArguments;
  });

  return evaluable;
}

std::vector<std::shared_ptr<AbstractExpression>> flatten_logical_expressions(
    const std::shared_ptr<AbstractExpression>& expression, const LogicalOperator logical_operator) {
  std::vector<std::shared_ptr<AbstractExpression>> flattened_expressions;

  visit_expression(expression, [&](const auto& sub_expression) {
    if (sub_expression->type == ExpressionType::Logical) {
      const auto logical_expression = std::static_pointer_cast<LogicalExpression>(sub_expression);
      if (logical_expression->logical_operator == logical_operator) {
        return ExpressionVisitation::VisitArguments;
      }
    }
    flattened_expressions.emplace_back(sub_expression);
    return ExpressionVisitation::DoNotVisitArguments;
  });

  return flattened_expressions;
}

std::shared_ptr<AbstractExpression> inflate_logical_expressions(
    const std::vector<std::shared_ptr<AbstractExpression>>& expressions, const LogicalOperator logical_operator) {
  auto inflated = std::shared_ptr<AbstractExpression>{};

  if (!expressions.empty()) {
    inflated = expressions.front();
  }

  const auto expression_count = expressions.size();
  for (auto expression_idx = size_t{1}; expression_idx < expression_count; ++expression_idx) {
    inflated = std::make_shared<LogicalExpression>(logical_operator, inflated, expressions[expression_idx]);
  }

  return inflated;
}

void expression_set_parameters(const std::shared_ptr<AbstractExpression>& expression,
                               const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  visit_expression(expression, [&](auto& sub_expression) {
    if (auto correlated_parameter_expression =
            std::dynamic_pointer_cast<CorrelatedParameterExpression>(sub_expression)) {
      const auto value_iter = parameters.find(correlated_parameter_expression->parameter_id);
      if (value_iter != parameters.end()) {
        correlated_parameter_expression->set_value(value_iter->second);
      }
      return ExpressionVisitation::DoNotVisitArguments;
    }

    if (const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
        pqp_subquery_expression) {
      pqp_subquery_expression->pqp->set_parameters(parameters);
      return ExpressionVisitation::DoNotVisitArguments;
    }

    return ExpressionVisitation::VisitArguments;
  });
}

void expressions_set_parameters(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  for (const auto& expression : expressions) {
    expression_set_parameters(expression, parameters);
  }
}

void expression_set_transaction_context(const std::shared_ptr<AbstractExpression>& expression,
                                        const std::weak_ptr<TransactionContext>& transaction_context) {
  visit_expression(expression, [&](auto& sub_expression) {
    if (sub_expression->type != ExpressionType::PQPSubquery) {
      return ExpressionVisitation::VisitArguments;
    }

    const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
    Assert(pqp_subquery_expression, "Expected a PQPSubqueryExpression here");
    pqp_subquery_expression->pqp->set_transaction_context_recursively(transaction_context);

    return ExpressionVisitation::DoNotVisitArguments;
  });
}

void expressions_set_transaction_context(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                                         const std::weak_ptr<TransactionContext>& transaction_context) {
  for (const auto& expression : expressions) {
    expression_set_transaction_context(expression, transaction_context);
  }
}

bool expression_contains_placeholder(const std::shared_ptr<AbstractExpression>& expression) {
  auto placeholder_found = false;

  visit_expression(expression, [&](const auto& sub_expression) {
    placeholder_found |= std::dynamic_pointer_cast<PlaceholderExpression>(sub_expression) != nullptr;
    return !placeholder_found ? ExpressionVisitation::VisitArguments : ExpressionVisitation::DoNotVisitArguments;
  });

  return placeholder_found;
}

bool expression_contains_correlated_parameter(const std::shared_ptr<AbstractExpression>& expression) {
  auto correlated_parameter_found = false;

  visit_expression(expression, [&](const auto& sub_expression) {
    correlated_parameter_found |= std::dynamic_pointer_cast<CorrelatedParameterExpression>(sub_expression) != nullptr;
    return !correlated_parameter_found ? ExpressionVisitation::VisitArguments
                                       : ExpressionVisitation::DoNotVisitArguments;
  });

  return correlated_parameter_found;
}

std::optional<AllTypeVariant> expression_get_value_or_parameter(const AbstractExpression& expression) {
  if (const auto* correlated_parameter_expression = dynamic_cast<const CorrelatedParameterExpression*>(&expression)) {
    DebugAssert(correlated_parameter_expression->value(), "CorrelatedParameterExpression doesn't have a value set");
    return *correlated_parameter_expression->value();
  }

  if (expression.type == ExpressionType::Value) {
    return static_cast<const ValueExpression&>(expression).value;
  }

  if (expression.type == ExpressionType::Cast) {
    const auto& cast_expression = static_cast<const CastExpression&>(expression);
    Assert(expression.data_type() != DataType::Null, "Cast as NULL is undefined");
    // More complicated casts  should be resolved by ExpressionEvaluator.
    // E.g., CAST(any_column AS INT) cannot and should not be evaluated here.
    if (cast_expression.argument()->type != ExpressionType::Value) {
      return std::nullopt;
    }
    const auto& value_expression = static_cast<const ValueExpression&>(*cast_expression.argument());

    // Casts from NULL are NULL
    if (variant_is_null(value_expression.value)) {
      return NULL_VALUE;
    }
    std::optional<AllTypeVariant> result;
    resolve_data_type(expression.data_type(), [&](auto type) {
      using TargetDataType = typename decltype(type)::type;
      try {
        // lossy_variant_cast returns std::nullopt when it casts from a NULL value. We have handled this above.
        result = *lossy_variant_cast<TargetDataType>(value_expression.value);
      } catch (boost::bad_lexical_cast&) {
        Fail("Cannot cast " + cast_expression.argument()->as_column_name() + " as " +
             std::string{magic_enum::enum_name(expression.data_type())});
      }
    });
    return result;
  }

  return std::nullopt;
}

std::vector<std::shared_ptr<PQPSubqueryExpression>> find_pqp_subquery_expressions(
    const std::shared_ptr<AbstractExpression>& expression) {
  if (const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(expression)) {
    // Quick Path
    return {pqp_subquery_expression};
  }

  // Long Path: Search expression's arguments for PQPSubqueryExpressions
  std::vector<std::shared_ptr<PQPSubqueryExpression>> pqp_subquery_expressions;
  for (const auto& argument_expression : expression->arguments) {
    visit_expression(argument_expression, [&](const auto& sub_expression) {
      const auto pqp_subquery_expression = std::dynamic_pointer_cast<PQPSubqueryExpression>(sub_expression);
      if (pqp_subquery_expression) {
        pqp_subquery_expressions.push_back(pqp_subquery_expression);
        return ExpressionVisitation::DoNotVisitArguments;
      }
      return ExpressionVisitation::VisitArguments;
    });
  }
  return pqp_subquery_expressions;
}

std::optional<ColumnID> find_expression_idx(const AbstractExpression& search_expression,
                                            const std::vector<std::shared_ptr<AbstractExpression>>& expression_vector) {
  const auto num_expressions = expression_vector.size();
  for (auto expression_id = ColumnID{0}; expression_id < num_expressions; ++expression_id) {
    if (search_expression == *expression_vector[expression_id]) {
      return expression_id;
    }
  }
  return std::nullopt;
}

template <typename ExpressionContainer>
bool contains_all_expressions(const ExpressionContainer& search_expressions,
                              const std::vector<std::shared_ptr<AbstractExpression>>& expression_vector) {
  if (search_expressions.size() > expression_vector.size()) {
    return false;
  }

  for (const auto& expression : search_expressions) {
    if (!std::any_of(expression_vector.cbegin(), expression_vector.cend(),
                     [&](const auto& output_expression) { return *output_expression == *expression; })) {
      return false;
    }
  }

  return true;
}

template bool contains_all_expressions<ExpressionUnorderedSet>(
    const ExpressionUnorderedSet& search_expressions,
    const std::vector<std::shared_ptr<AbstractExpression>>& expression_vector);

template bool contains_all_expressions<std::vector<std::shared_ptr<AbstractExpression>>>(
    const std::vector<std::shared_ptr<AbstractExpression>>& search_expressions,
    const std::vector<std::shared_ptr<AbstractExpression>>& expression_vector);

}  // namespace hyrise
