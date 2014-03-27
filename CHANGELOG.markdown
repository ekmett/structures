0.2
---
* `Data.Vector.Map` now has asymptotics that are fully deamortized.
* `Data.Vector.Map.Ephemeral` provides a cache-oblivious lookahead array that doesn't deamortize.
  On the plus side it can be 2-4x faster than `Data.Vector.Map`.
  On the downside, using anything but the most recent version can dramaticlly affect the asymptotics of your program.

0.1
---
* Repository initialized

