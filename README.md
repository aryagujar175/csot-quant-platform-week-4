This project implements a streaming pipeline: a market-data thread decodes a tick feed and hands each tick to a strategy thread across a lock-free single-producer/single-consumer ring buffer — no mutex, no allocation, the right memory orderings, and the two stages overlapping so the handoff latency vanishes. This was created as a part of CAIC Summer of Tech (CSOT) IIT Delhi, Low- Latency Track. This is (as of 29 June 2026), the fourth week's output.

Headline numbers: on judge: Runtime = 39,212,289,184 ns (200,000,000 ticks, best of 3 runs), Thoroughput: 5.1 M ticks/s
(This week's judge was remarkably inconsistent, so the same strategy gave a runtime of 39 seconds on one of the runs and 52 seconds on the other)

Build steps (in Linux Only): 
build && cmake -B build && cmake --build build -j
./build/pipeline_runner data/large.feed

(use this instruction to generate large.feed: python3 data/gen_feed.py --accesses 5000000 --seed 42 --out data/large.feed)

I have attached perf numbers below, but I am not sure they make sense. I strongly doubt that the program was running on 2 seperate cores on my laptop, and all the methods I tried to actually verify that were futile.

 Performance counter stats for './build/pipeline_runner data/large.feed':

         4,178,181      cpu_atom/cache-misses/                                                  (44.37%)
        26,437,218      cpu_core/cache-misses/                                                  (55.63%)
             1,011      context-switches                                                      
     6,571,117,388      cpu_atom/instructions/                                                  (44.37%)
     9,940,838,112      cpu_core/instructions/                                                  (55.63%)

      14.240745290 seconds time elapsed

       2.290504000 seconds user
       4.748826000 seconds sys

