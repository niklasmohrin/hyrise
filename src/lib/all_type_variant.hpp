#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <boost/bimap.hpp>
#include <boost/hana/core/to.hpp>
#include <boost/hana/ext/boost/mpl/vector.hpp>
#include <boost/hana/fold.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/prepend.hpp>
#include <boost/hana/transform.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/zip.hpp>
#include <boost/mpl/push_front.hpp>
#include <boost/preprocessor/seq/enum.hpp>
#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/seq/size.hpp>
#include <boost/preprocessor/seq/transform.hpp>
#include <boost/variant.hpp>

#include "null_value.hpp"
#include "types.hpp"

namespace hyrise {

namespace hana = boost::hana;

namespace detail {

// clang-format off
#define DATA_TYPE_INFO                 \
  ((int32_t,    Int,        "int"))    \
  ((int64_t,    Long,       "long"))   \
  ((float,      Float,      "float"))  \
  ((double,     Double,     "double")) \
  ((pmr_string, String,     "string"))
//  Type        Enum Value   String
// clang-format on

#define NUM_DATA_TYPES BOOST_PP_SEQ_SIZE(DATA_TYPE_INFO)

#define GET_ELEM(s, index, elem) BOOST_PP_TUPLE_ELEM(NUM_DATA_TYPES, index, elem)
#define APPEND_ENUM_NAMESPACE(s, d, enum_value) DataType::enum_value

#define DATA_TYPES BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 0, DATA_TYPE_INFO)
#define DATA_TYPE_ENUM_VALUES BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 1, DATA_TYPE_INFO)
#define DATA_TYPE_STRINGS BOOST_PP_SEQ_TRANSFORM(GET_ELEM, 2, DATA_TYPE_INFO)

enum class DataType : uint8_t { Null, BOOST_PP_SEQ_ENUM(DATA_TYPE_ENUM_VALUES) };

static constexpr auto data_types = hana::to_tuple(hana::tuple_t<BOOST_PP_SEQ_ENUM(DATA_TYPES)>);
static constexpr auto data_type_enum_values =
    hana::make_tuple(BOOST_PP_SEQ_ENUM(BOOST_PP_SEQ_TRANSFORM(APPEND_ENUM_NAMESPACE, _, DATA_TYPE_ENUM_VALUES)));
static constexpr auto data_type_strings = hana::make_tuple(BOOST_PP_SEQ_ENUM(DATA_TYPE_STRINGS));

constexpr auto to_pair = [](auto tuple) { return hana::make_pair(hana::at_c<0>(tuple), hana::at_c<1>(tuple)); };

static constexpr auto data_type_enum_pairs = hana::transform(hana::zip(data_type_enum_values, data_types), to_pair);
static constexpr auto data_type_enum_string_pairs =
    hana::transform(hana::zip(data_type_enum_values, data_type_strings), to_pair);

// Prepends NullValue to tuple of types
static constexpr auto data_types_including_null = hana::prepend(data_types, hana::type_c<NullValue>);

// Converts tuple to mpl vector
using TypesAsMplVector = decltype(hana::to<hana::ext::boost::mpl::vector_tag>(data_types_including_null));

// Creates boost::variant from mpl vector
using AllTypeVariant = typename boost::make_variant_over<detail::TypesAsMplVector>::type;

}  // namespace detail

static constexpr auto data_types = detail::data_types;
static constexpr auto data_types_including_null = detail::data_types_including_null;
static constexpr auto data_type_pairs = detail::data_type_enum_pairs;
static constexpr auto data_type_enum_string_pairs = detail::data_type_enum_string_pairs;

using DataType = detail::DataType;
using AllTypeVariant = detail::AllTypeVariant;

// Function to check if AllTypeVariant is null
inline bool variant_is_null(const AllTypeVariant& variant) {
  return (variant.which() == 0);
}

const auto data_type_to_string =
    hana::fold(data_type_enum_string_pairs, boost::bimap<DataType, std::string>{}, [](auto map, auto pair) {
      map.insert({hana::first(pair), std::string{hana::second(pair)}});
      return map;
    });

std::ostream& operator<<(std::ostream& stream, const DataType data_type);

bool is_floating_point_data_type(const DataType data_type);

/**
 * Notes:
 *   – Use this instead of AllTypeVariant{}, AllTypeVariant{NullValue{}}, NullValue{}, etc.
 *     whenever a null value needs to be represented
 *   - comparing any AllTypeVariant to NULL_VALUE returns false in accordance with the ternary logic
 *   - use variant_is_null() if you want to check if an AllTypeVariant is null
 */
static const auto NULL_VALUE = AllTypeVariant{};

/**
 * @defgroup Macros for explicitly instantiating template classes
 *
 * In order to improve compile times, we explicitly instantiate
 * template classes which are going to be used with column types.
 * Because we do not want any redundant lists of column types spread
 * across the code base, we use EXPLICITLY_INSTANTIATE_DATA_TYPES.
 *
 * @{
 */

#define EXPLICIT_DECLARATION(r, template_class, type) extern template class template_class<type>;

// Explicitly declares the given template class for all types in DATA_TYPES (used in .hpp)
#define EXPLICITLY_DECLARE_DATA_TYPES(template_class)                     \
  BOOST_PP_SEQ_FOR_EACH(EXPLICIT_DECLARATION, template_class, DATA_TYPES) \
  static_assert(true, "End call of macro with a semicolon")

#define EXPLICIT_INSTANTIATION(r, template_class, type) template class template_class<type>;

// Explicitly instantiates the given template class for all types in DATA_TYPES (used in .cpp)
#define EXPLICITLY_INSTANTIATE_DATA_TYPES(template_class)                   \
  BOOST_PP_SEQ_FOR_EACH(EXPLICIT_INSTANTIATION, template_class, DATA_TYPES) \
  static_assert(true, "End call of macro with a semicolon")

/**
 * This function returns the DataType of an AllTypeVariant
 *
 * Note: DataType and AllTypeVariant are defined in a way such that
 *       the indices in DataType and AllTypeVariant match.
 */
inline DataType data_type_from_all_type_variant(const AllTypeVariant& all_type_variant) {
  return static_cast<DataType>(all_type_variant.which());
}

template <typename T>
std::optional<T> as_optional(AllTypeVariant value) {
  if (variant_is_null(value)) {
    return std::nullopt;
  }
  return boost::get<T>(value);
}

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::AllTypeVariant> {
  size_t operator()(const hyrise::AllTypeVariant& all_type_variant) const;
};

}  // namespace std
