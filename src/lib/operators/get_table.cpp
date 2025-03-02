#include "get_table.hpp"

#include <algorithm>
#include <sstream>
#include <unordered_set>

#include "hyrise.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/table_scan.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "utils/pruning_utils.hpp"

namespace hyrise {

GetTable::GetTable(const std::string& name) : GetTable{name, {}, {}} {}

GetTable::GetTable(const std::string& name, const std::vector<ChunkID>& pruned_chunk_ids,
                   const std::vector<ColumnID>& pruned_column_ids)
    : AbstractReadOnlyOperator{OperatorType::GetTable},
      _name{name},
      _pruned_chunk_ids{pruned_chunk_ids},
      _pruned_column_ids{pruned_column_ids} {
  // Check pruned_chunk_ids
  DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()), "Expected sorted vector of ChunkIDs");
  DebugAssert(std::adjacent_find(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()) == _pruned_chunk_ids.end(),
              "Expected vector of unique ChunkIDs");

  // Check pruned_column_ids
  DebugAssert(std::is_sorted(_pruned_column_ids.begin(), _pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");
  DebugAssert(std::adjacent_find(_pruned_column_ids.begin(), _pruned_column_ids.end()) == _pruned_column_ids.end(),
              "Expected vector of unique ColumnIDs");
}

const std::string& GetTable::name() const {
  static const auto name = std::string{"GetTable"};
  return name;
}

std::string GetTable::description(DescriptionMode description_mode) const {
  const auto stored_table = Hyrise::get().storage_manager.get_table(_name);
  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  auto stream = std::stringstream{};

  stream << AbstractOperator::description(description_mode) << separator;
  stream << "(" << table_name() << ")" << separator;
  stream << "pruned:" << separator;
  stream << _pruned_chunk_ids.size() << "/" << stored_table->chunk_count() << " chunk(s)";
  if (description_mode == DescriptionMode::SingleLine) {
    stream << ",";
  }
  stream << separator;
  stream << _pruned_column_ids.size() << "/" << stored_table->column_count() << " column(s)";

  return stream.str();
}

const std::string& GetTable::table_name() const {
  return _name;
}

const std::vector<ChunkID>& GetTable::pruned_chunk_ids() const {
  return _pruned_chunk_ids;
}

const std::vector<ColumnID>& GetTable::pruned_column_ids() const {
  return _pruned_column_ids;
}

std::shared_ptr<AbstractOperator> GetTable::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<GetTable>(_name, _pruned_chunk_ids, _pruned_column_ids);
}

void GetTable::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> GetTable::_on_execute() {
  const auto stored_table = Hyrise::get().storage_manager.get_table(_name);

  // The chunk count might change while we are in this method as other threads concurrently insert new data. MVCC
  // guarantees that rows that are inserted after this transaction was started (and thus after GetTable started to
  // execute) are not visible. Thus, we do not have to care about chunks added after this point. By retrieving
  // chunk_count only once, we avoid concurrency issues, for example when more chunks are added to output_chunks than
  // entries were originally allocated.
  const auto chunk_count = stored_table->chunk_count();

  /**
   * Build a sorted vector (`excluded_chunk_ids`) of physically/logically deleted and pruned ChunkIDs
   */
  DebugAssert(!transaction_context_is_set() || transaction_context()->phase() == TransactionPhase::Active,
              "Transaction is not active anymore.");
  if (HYRISE_DEBUG && !transaction_context_is_set()) {
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      DebugAssert(stored_table->get_chunk(chunk_id) && !stored_table->get_chunk(chunk_id)->get_cleanup_commit_id(),
                  "For TableType::Data tables with deleted chunks, the transaction context must be set.");
    }
  }

  // Currently, value_clustered_by is only used for temporary tables. If tables in the StorageManager start using that
  // flag, too, it needs to be forwarded here; otherwise it would be completely invisible in the PQP.
  DebugAssert(stored_table->value_clustered_by().empty(), "GetTable does not forward value_clustered_by");
  auto excluded_chunk_ids = std::vector<ChunkID>{};
  auto pruned_chunk_ids_iter = _pruned_chunk_ids.begin();
  for (ChunkID stored_chunk_id{0}; stored_chunk_id < chunk_count; ++stored_chunk_id) {
    // Check whether the Chunk is pruned
    if (pruned_chunk_ids_iter != _pruned_chunk_ids.end() && *pruned_chunk_ids_iter == stored_chunk_id) {
      ++pruned_chunk_ids_iter;
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    const auto chunk = stored_table->get_chunk(stored_chunk_id);

    // Skip chunks that were physically deleted
    if (!chunk) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    // Skip chunks that were just inserted by a different transaction and that do not have any content yet
    if (chunk->size() == 0) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }

    // Check whether the Chunk is logically deleted
    if (transaction_context_is_set() && chunk->get_cleanup_commit_id() &&
        *chunk->get_cleanup_commit_id() <= transaction_context()->snapshot_commit_id()) {
      excluded_chunk_ids.emplace_back(stored_chunk_id);
      continue;
    }
  }

  // We cannot create a Table without columns - since Chunks rely on their first column to determine their row count
  Assert(_pruned_column_ids.size() < static_cast<size_t>(stored_table->column_count()),
         "Cannot prune all columns from Table");
  DebugAssert(std::all_of(_pruned_column_ids.begin(), _pruned_column_ids.end(),
                          [&](const auto column_id) { return column_id < stored_table->column_count(); }),
              "ColumnID out of range");

  /**
   * Build pruned TableColumnDefinitions of the output Table
   */
  auto pruned_column_definitions = TableColumnDefinitions{};
  if (_pruned_column_ids.empty()) {
    pruned_column_definitions = stored_table->column_definitions();
  } else {
    pruned_column_definitions =
        TableColumnDefinitions{stored_table->column_definitions().size() - _pruned_column_ids.size()};

    auto pruned_column_ids_iter = _pruned_column_ids.begin();
    for (auto stored_column_id = ColumnID{0}, output_column_id = ColumnID{0};
         stored_column_id < stored_table->column_count(); ++stored_column_id) {
      if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
        ++pruned_column_ids_iter;
        continue;
      }

      pruned_column_definitions[output_column_id] = stored_table->column_definitions()[stored_column_id];
      ++output_column_id;
    }
  }

  /**
   * Build the output Table, omitting pruned Chunks and Columns as well as deleted Chunks
   */
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count - excluded_chunk_ids.size()};
  auto output_chunks_iter = output_chunks.begin();

  auto excluded_chunk_ids_iter = excluded_chunk_ids.begin();

  for (auto stored_chunk_id = ChunkID{0}; stored_chunk_id < chunk_count; ++stored_chunk_id) {
    // Skip `stored_chunk_id` if it is in the sorted vector `excluded_chunk_ids`
    if (excluded_chunk_ids_iter != excluded_chunk_ids.end() && *excluded_chunk_ids_iter == stored_chunk_id) {
      ++excluded_chunk_ids_iter;
      continue;
    }

    // The Chunk is to be included in the output Table, now we progress to excluding Columns
    const auto stored_chunk = stored_table->get_chunk(stored_chunk_id);

    // Make a copy of the order-by information of the current chunk. This information is adapted when columns are
    // pruned and will be set on the output chunk.
    const auto& input_chunk_sorted_by = stored_chunk->individually_sorted_by();
    auto chunk_sort_definition = std::optional<SortColumnDefinition>{};

    if (_pruned_column_ids.empty()) {
      *output_chunks_iter = stored_chunk;
    } else {
      const auto column_count = stored_table->column_count();
      auto output_segments = Segments{column_count - _pruned_column_ids.size()};
      auto output_segments_iter = output_segments.begin();
      auto output_indexes = Indexes{};

      auto pruned_column_ids_iter = _pruned_column_ids.begin();
      for (auto stored_column_id = ColumnID{0}; stored_column_id < column_count; ++stored_column_id) {
        // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
        if (pruned_column_ids_iter != _pruned_column_ids.end() && stored_column_id == *pruned_column_ids_iter) {
          ++pruned_column_ids_iter;
          continue;
        }

        if (!input_chunk_sorted_by.empty()) {
          for (const auto& sorted_by : input_chunk_sorted_by) {
            if (sorted_by.column == stored_column_id) {
              const auto columns_pruned_so_far = std::distance(_pruned_column_ids.begin(), pruned_column_ids_iter);
              const auto new_sort_column =
                  ColumnID{static_cast<uint16_t>(static_cast<size_t>(stored_column_id) - columns_pruned_so_far)};
              chunk_sort_definition = SortColumnDefinition(new_sort_column, sorted_by.sort_mode);
            }
          }
        }

        *output_segments_iter = stored_chunk->get_segment(stored_column_id);
        auto indexes = stored_chunk->get_indexes({*output_segments_iter});
        if (!indexes.empty()) {
          output_indexes.insert(std::end(output_indexes), std::begin(indexes), std::end(indexes));
        }
        ++output_segments_iter;
      }

      *output_chunks_iter = std::make_shared<Chunk>(std::move(output_segments), stored_chunk->mvcc_data(),
                                                    stored_chunk->get_allocator(), std::move(output_indexes));

      if (!stored_chunk->is_mutable()) {
        // Finalizing is cheap here: the MvccData's max_begin_cid is already set, so finalize() only sets the flag and
        // does not trigger anything else.
        (*output_chunks_iter)->finalize();
      }

      if (chunk_sort_definition) {
        (*output_chunks_iter)->set_individually_sorted_by(*chunk_sort_definition);
      }

      // The output chunk contains all rows that are in the stored chunk, including invalid rows. We forward this
      // information so that following operators (currently, the Validate operator) can use it for optimizations.
      (*output_chunks_iter)->increase_invalid_row_count(stored_chunk->invalid_row_count());
    }

    ++output_chunks_iter;
  }

  // Lambda to check if all chunks indexed by a table index have been pruned by the ChunkPruningRule or
  // ColumnPruningRule of the optimizer.
  const auto all_indexed_segments_pruned = [&](const auto& table_index) {
    // Check if indexed ColumnID has been pruned.
    const auto indexed_column_id = table_index->get_indexed_column_id();
    if (std::find(_pruned_column_ids.cbegin(), _pruned_column_ids.cend(), indexed_column_id) !=
        _pruned_column_ids.cend()) {
      return true;
    }

    const auto indexed_chunk_ids = table_index->get_indexed_chunk_ids();

    // Early out if index is empty.
    if (indexed_chunk_ids.empty()) {
      return false;
    }

    // Check if the indexed chunks have been pruned.
    DebugAssert(std::is_sorted(_pruned_chunk_ids.begin(), _pruned_chunk_ids.end()),
                "Expected _pruned_chunk_ids vector to be sorted.");
    return std::all_of(indexed_chunk_ids.cbegin(), indexed_chunk_ids.cend(), [&](const auto chunk_id) {
      return std::binary_search(_pruned_chunk_ids.cbegin(), _pruned_chunk_ids.cend(), chunk_id);
    });
  };

  auto table_indexes = stored_table->get_table_indexes();
  table_indexes.erase(std::remove_if(table_indexes.begin(), table_indexes.end(), all_indexed_segments_pruned),
                      table_indexes.cend());

  return std::make_shared<Table>(pruned_column_definitions, TableType::Data, std::move(output_chunks),
                                 stored_table->uses_mvcc(), table_indexes);
}

}  // namespace hyrise
