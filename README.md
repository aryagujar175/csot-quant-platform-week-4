This project implements a streaming pipeline: a market-data thread decodes a tick feed and hands each tick to a strategy thread across a lock-free single-producer/single-consumer ring buffer — no mutex, no allocation, the right memory orderings, and the two stages overlapping so the handoff latency vanishes. This was created as a part of CAIC Summer of Tech (CSOT) IIT Delhi, Low- Latency Track. This is (as of 29 June 2026), the fourth week's output.

Headline numbers: on judge: Runtime = 39,212,289,184 ns (200,000,000 ticks, best of 3 runs), Thoroughput: 5.1 M ticks/s
(This week's judge was remarkably inconsistent, so the same strategy gave a runtime of 39 seconds on one of the runs and 52 seconds on the other)

Build steps (in Linux Only): 
build && cmake -B build && cmake --build build -j
./build/pipeline_runner data/large.feed

(use this instruction to generate large.feed: python3 data/gen_feed.py --accesses 5000000 --seed 42 --out data/large.feed)

I have attached perf numbers below, but I am not sure they make sense. I strongly doubt that the program was running on 2 seperate cores on my laptop, and all the methods I tried to actually verify that were futile.

 
 Performance counter stats for './build/pipeline_runner data/large.feed':

         3,709,676      cpu_atom/cache-misses/                                                  (35.86%)
        25,394,663      cpu_core/cache-misses/                                                  (64.14%)
               819      context-switches                                                      
     6,671,721,208      cpu_atom/instructions/                                                  (35.86%)
     9,432,773,050      cpu_core/instructions/                                                  (64.14%)

       9.271997406 seconds time elapsed

       1.130777000 seconds user
       3.225777000 seconds sys

Things that I learnt/ surprised me:

1. Using a rolling sum for calculating variance did not work. Even with recalculating the entire sum every 1024th iteration of the symbol. Decimal drift was likely too large somewhere in the hidden cases.
2. It was really hard for me to understand what was working and what was not this week, because of the inherent inconsistency of the judge. 
3. At first, I was really surprised by the division between the two threads: one thread will just process the tick while the other thread does most of the heavy lifting. Despite what I thought, this combination of threads significantly outperformed a single thread.
