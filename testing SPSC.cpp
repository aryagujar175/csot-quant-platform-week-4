#include "pipeline.hpp"
#include <atomic>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>
#include <thread>
using namespace std;

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
    std::uint32_t count = 0;     // valid mids seen so far, capped at WINDOW
    std::uint32_t head = 0;      // next write index into mids[]
    std::int32_t  position = 0;  // -1, 0, or +1
};

struct TickAl{
    size_t i = 0;
    uint32_t sym = 999;
    double bid_px = 0.0;
    double ask_px = 0.0;
};

class StubPipeline final : public csot::Pipeline {
    std::uint32_t                 num_symbols_ = 0;
    std::vector<std::string>      names_;   // names_[k] == "SYM<k>" (interned once)
    std::vector<SymbolState>      state_;
    SpscRing<TickAl, 1024> ring;

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
            for (size_t i = 0; i < n; ++i) {
                const csot::WireTick& w = in[i];
                TickAl tk;
                tk.i = i;
                tk.sym = w.symbol_id;
                tk.bid_px = static_cast<double>(w.bid_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                tk.ask_px = static_cast<double>(w.ask_px_fp) /
                                  static_cast<double>(csot::PRICE_SCALE);
                while (!ring.try_push(tk)) { /* spin: consumer is behind */ }
            }
            done.store(true, memory_order_release);
        });

        thread cons([&]() {
                TickAl tk;
                for (;;) {
                    if (ring.try_pop(tk)) {
                        SymbolState& st = state_[tk.sym];

            const double mid = (tk.bid_px + tk.ask_px) * 0.5;
            st.mids[st.head] = mid;
            st.head = (st.head + 1) & (WINDOW - 1);   // valid because WINDOW == 64
            if (st.count < WINDOW) {
                ++st.count;
            }
            if (st.count < WINDOW) {
                continue;  // warm-up: no order
            }

            double sum = 0.0;
            for (double x : st.mids) sum += x;
            const double mean = sum / static_cast<double>(WINDOW);

            double sq = 0.0;
            for (double x : st.mids) {
                const double d = x - mean;
                sq += d * d;
            }
            const double variance = sq / static_cast<double>(WINDOW);
            const double stddev = std::sqrt(variance);
            if (stddev < EPSILON_STDDEV) {
                continue;
            }

            const double z = (mid - mean) / stddev;
            const double abs_z = std::fabs(z);

            // ---- Emit (and apply the deterministic fill, STRATEGY_SPEC §8) --
            csot::Order order{};
            bool emit = false;

            if (st.position == 0) {
                if (z >= ENTRY_Z) {
                    order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px, 1};
                    st.position -= 1;
                    emit = true;
                } else if (z <= -ENTRY_Z) {
                    order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px, 1};
                    st.position += 1;
                    emit = true;
                }
            } else if (st.position > 0 && abs_z <= EXIT_Z) {
                order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px,
                         static_cast<std::uint32_t>(st.position)};
                st.position = 0;
                emit = true;
            } else if (st.position < 0 && abs_z <= EXIT_Z) {
                order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px,
                         static_cast<std::uint32_t>(-st.position)};
                st.position = 0;
                emit = true;
            }

            if (emit) {
                out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                out[num_orders].order = order;
                ++num_orders;
            }
                    } else if (done.load(memory_order_acquire)) {
                        if (!ring.try_pop(tk)) break;
                        SymbolState& st = state_[tk.sym];

            const double mid = (tk.bid_px + tk.ask_px) * 0.5;
            st.mids[st.head] = mid;
            st.head = (st.head + 1) & (WINDOW - 1);   // valid because WINDOW == 64
            if (st.count < WINDOW) {
                ++st.count;
            }
            if (st.count < WINDOW) {
                continue;  // warm-up: no order
            }

            double sum = 0.0;
            for (double x : st.mids) sum += x;
            const double mean = sum / static_cast<double>(WINDOW);

            double sq = 0.0;
            for (double x : st.mids) {
                const double d = x - mean;
                sq += d * d;
            }
            const double variance = sq / static_cast<double>(WINDOW);
            const double stddev = std::sqrt(variance);
            if (stddev < EPSILON_STDDEV) {
                continue;
            }

            const double z = (mid - mean) / stddev;
            const double abs_z = std::fabs(z);

            // ---- Emit (and apply the deterministic fill, STRATEGY_SPEC §8) --
            csot::Order order{};
            bool emit = false;

            if (st.position == 0) {
                if (z >= ENTRY_Z) {
                    order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px, 1};
                    st.position -= 1;
                    emit = true;
                } else if (z <= -ENTRY_Z) {
                    order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px, 1};
                    st.position += 1;
                    emit = true;
                }
            } else if (st.position > 0 && abs_z <= EXIT_Z) {
                order = {csot::Order::Side::SELL, names_[tk.sym], tk.bid_px,
                         static_cast<std::uint32_t>(st.position)};
                st.position = 0;
                emit = true;
            } else if (st.position < 0 && abs_z <= EXIT_Z) {
                order = {csot::Order::Side::BUY, names_[tk.sym], tk.ask_px,
                         static_cast<std::uint32_t>(-st.position)};
                st.position = 0;
                emit = true;
            }

            if (emit) {
                out[num_orders].tick_index = static_cast<std::uint64_t>(tk.i);
                out[num_orders].order = order;
                ++num_orders;
            }
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
