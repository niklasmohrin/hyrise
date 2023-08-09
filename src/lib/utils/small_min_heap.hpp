#pragma once

#include <array>
#include <functional>
#include <ranges>

#include "assert.hpp"

namespace hyrise {

template <uint8_t max_size, typename T, typename Compare = std::less<T>>
class SmallMinHeap {
 public:
  template <typename R>
    requires std::ranges::input_range<R> && std::ranges::sized_range<R>
  explicit SmallMinHeap(R&& initial_elements, Compare init_compare = {}) : _compare(std::move(init_compare)) {
    const auto size = std::ranges::size(initial_elements);
    Assert(size <= max_size, "SmallMinHeap got more than max_size initial elements");
    _size = static_cast<uint8_t>(size);
    std::ranges::move(initial_elements, _elements.begin());
    std::ranges::sort(std::span(_elements).subspan(0, _size), _compare);
  }

  explicit SmallMinHeap(Compare init_compare = {}) : SmallMinHeap(std::array<T, 0>{}, std::move(init_compare)) {}

  uint8_t size() const {
    return _size;
  }

  bool empty() const {
    return size() == 0;
  }

  void push(T element) {
    DebugAssert(size() < max_size, "Pushed into already full SmallMinHeap");

    const auto insertion_point =
        std::ranges::partition_point(_elements.begin(), _elements.begin() + _size,
                                     [&](const auto& contained) { return !_compare(element, contained); });
    std::ranges::move_backward(insertion_point, _elements.begin() + _size, _elements.begin() + _size + 1);
    *insertion_point = std::move(element);
    ++_size;
  }

  const T& top() const {
    return _elements.front();
  }

  T pop() {
    DebugAssert(size() > 0, "Popped from empty SmallMinHeap");
    auto result = std::move(_elements.front());
    std::ranges::move(_elements.begin() + 1, _elements.begin() + _size, _elements.begin());
    --_size;
    return result;
  }

 private:
  std::array<T, max_size> _elements;
  Compare _compare;
  uint8_t _size;
};

}  // namespace hyrise
