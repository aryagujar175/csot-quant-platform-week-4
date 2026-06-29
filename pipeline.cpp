#include "pipeline.hpp"
#include <atomic>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>
#include <thread>
#include <iostream>
#include <fstream>
#include <bits/stdc++.h>
#include <chrono>
using namespace std;
#include <pthread.h>
#include <sched.h>
#include <cstdlib>

void pin_to(int core) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    sched_setaffinity(0, sizeof(set), &set);
}
// producer: pin_to(2);   consumer: pin_to(3);   (two distinct cores)

template <typename T, std::size_t Capacity>   // Capacity MUST be a power of two
class SpscRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static constexpr std::size_t kMask = Capacity - 1;
    static constexpr std::size_t kLine = 64;

    T slots_[Capacity];
    alignas(kLine) std::atomic<std::size_t> tail_{0};   // producer writes
    alignas(kLine) std::atomic<std::size_t> head_{0};   // consumer writes

public:
    alignas(64) atomic<bool> done{false};
    // PRODUCER ONLY. Returns false if the queue is full (back-pressure).
    bool try_push(const T& item) {
        const std::size_t t = tail_.load(std::memory_order_relaxed);   // own index: relaxed
        if (t - head_.load(std::memory_order_acquire) == Capacity)     // other's: acquire
            return false;                                              // full
        slots_[t & kMask] = item;                                      // write payload
        tail_.store(t + 1, std::memory_order_release);                 // publish (release)
        return true;
    }

    // CONSUMER ONLY. Returns false if the queue is empty.
    bool try_pop(T& out) {
        const std::size_t h = head_.load(std::memory_order_relaxed);   // own index: relaxed
        if (h == tail_.load(std::memory_order_acquire))                // other's: acquire
            return false;                                              // empty
        out = slots_[h & kMask];                                       // read payload
        head_.store(h + 1, std::memory_order_release);                 // publish (release)
        return true;
    }
};

namespace {

// --- Frozen strategy constants (STRATEGY_SPEC.md §3) ------------------------
constexpr std::uint32_t WINDOW = 64;
constexpr double        ENTRY_Z = 2.0;
constexpr double        EXIT_Z = 0.5;
constexpr double        EPSILON_STDDEV = 1e-9;

// Per-symbol rolling state (STRATEGY_SPEC.md §4). One of these per symbol id.
struct SymbolState {
    double        mids[WINDOW] = {};
    double        sum = 0.0;     // rolling sum of mids[]
    std::uint32_t count = 0;     // valid mids seen so far, capped at WINDOW
    std::uint32_t head = 0;      // next write index into mids[]
    std::int32_t  position = 0;  // -1, 0, or +1
};

struct alignas(64) TickAl{
    size_t i = 0;
    uint32_t sym = 999;
    double bid_px = 0.0;
    double ask_px = 0.0;
    double mid = 0.0;
};

class StubPipeline final : public csot::Pipeline {
    std::uint32_t                 num_symbols_ = 0;
    std::vector<std::string>      names_;   // names_[k] == "SYM<k>" (interned once)
    std::vector<SymbolState>      state_;
    SpscRing<TickAl, 65536> ring;

    // The (previously duplicated) per-tick strategy body, factored into a
    // single function and rewritten using the spec_strategy optimisations.
    __attribute__((always_inline)) inline
    void process_tick(const TickAl& tk, csot::OrderRecord* out,
                      std::size_t& num_orders) {
        SymbolState& st = state_[tk.sym];

        const double oldest_mid = st.mids[st.head];
        const double diff = tk.mid - oldest_mid;
        st.mids[st.head] = tk.mid;
        st.head = (st.head + 1) & (WINDOW - 1);   // valid because WINDOW == 64
        st.sum += diff;

        /*if ((st.count & 1023) == 0) {
            double full = 0.0;
            for (double x : st.mids) full += x;
            st.sum = full;
        }*/
        ++st.count;

        if (st.count < WINDOW) [[unlikely]] {
            return;  // warm-up: no order
        }


        const double sum = st.sum;
        const double mean = sum * 0.015625;

        double sq = 0.0;
        for (double x : st.mids) {
            const double d = x - mean;
            sq += d * d;
        }
        const double variance = sq * 0.015625;

        // sq == sqsum - sum^2/WINDOW == WINDOW * variance, which is exactly the
        // spec_strategy "rhs". Using it lets us drop the sqrt and the division.
        const double rhs = sq;

        /*if (rhs < 64e-18) {
            return;
        }*/

        if (st.position == 0) [[likely]] {
            const double lhs_base_16 = tk.mid * 4.0 - sum * 0.0625;
            if (lhs_base_16 * lhs_base_16 >= rhs) {
                if (tk.mid * 64.0 > sum) {
                    out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                    out[num_orders].order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px, 1};
                    ++num_orders;
                    st.position -= 1;
                } else {
                    out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                    out[num_orders].order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px, 1};
                    ++num_orders;
                    st.position += 1;
                }
            }
            return;
        }

        const double lhs_base_256 = tk.mid * 16.0 - sum * 0.25;
        if (lhs_base_256 * lhs_base_256 <= rhs) {
            if (st.position > 0) {
                out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                out[num_orders].order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px,
                                         static_cast<std::uint32_t>(st.position)};
                ++num_orders;
            } else {
                out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                out[num_orders].order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px,
                                         static_cast<std::uint32_t>(-st.position)};
                ++num_orders;
            }
            st.position = 0;
        }
    }

public:
    void on_init(std::uint32_t num_symbols) override {
        // COLD PATH: allocate everything you will ever need here. For the fast
        // version that means your ring-buffer storage and thread bookkeeping
        // too — never inside run().
        num_symbols_ = num_symbols;
        names_.resize(num_symbols);
        for (std::uint32_t k = 0; k < num_symbols; ++k) {
            names_[k] = "SYM" + std::to_string(k);
        }
        state_.assign(num_symbols, SymbolState{});
    }

    std::size_t run(const csot::WireTick* in, std::size_t n,
                    csot::OrderRecord* out) override {
        std::size_t num_orders = 0;

        // SINGLE-THREADED: decode each tick, then strategize, in stream order.
        // TODO: split this into a producer thread (the decode below) and a
        // consumer thread (the strategy below) connected by your SPSC queue.
        atomic<bool> done{false};

        thread prod([&]() {
            pin_to(2);
            #pragma GCC unroll 8
            for (size_t i = 0; i < n-24; ++i) {
                __builtin_prefetch(&in[i + 24]);
                const csot::WireTick& w = in[i];
                TickAl tk;
                tk.i = i;
                tk.sym = w.symbol_id;
                tk.bid_px = static_cast<double>(w.bid_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                tk.ask_px = static_cast<double>(w.ask_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                tk.mid = (tk.bid_px + tk.ask_px) * 0.5;
                while (!ring.try_push(tk)) { /* spin: consumer is behind */ }
            }

            #pragma GCC unroll 8
            for (size_t i = n-24; i < n; ++i) {
                const csot::WireTick& w = in[i];
                TickAl tk;
                tk.i = i;
                tk.sym = w.symbol_id;
                tk.bid_px = static_cast<double>(w.bid_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                tk.ask_px = static_cast<double>(w.ask_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                tk.mid = (tk.bid_px + tk.ask_px) * 0.5;
                while (!ring.try_push(tk)) { /* spin: consumer is behind */ }
            }
            done.store(true, memory_order_release);
        });

        thread cons([&]() {
            pin_to(3);
                TickAl tk;
                for (;;) {
                    if (ring.try_pop(tk)) {
                        process_tick(tk, out, num_orders);
                    } else if (done.load(memory_order_acquire)) {
                        if (!ring.try_pop(tk)) break;
                        process_tick(tk, out, num_orders);
                    } else { /*eat 5 star do nothing*/ }
                }
        });

        prod.join();
        cons.join();

        return num_orders;
    }
};

}  // namespace

extern "C" csot::Pipeline* create_pipeline() {
    return new StubPipeline();
}