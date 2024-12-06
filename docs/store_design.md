# Random Walk Store Design

The following data structures are used to implement the RandomWalkStore interface using Redis as the in-memory datastore.

#### RWS

The `RWS` is a Redis hash that encapsulate relevant metadata about the RandomWalkStore. The field `lastWalkID` is used to assign unique IDs to new walks.

```
RWS = HASH {     alpha: <alpha>,
                 walksPerNode: <walksPerNode>,
                 lastWalkID: <walkID>
                 totalVisits: <totalVisits>
            }
```

---

#### walks

The `walks` is a Redis hash that maps each walkID to a walk, which is a strings of nodeIDs separated by commas e.g.  `"0,1,2,3,4"`.

```
walks = HASH { <walkID>: <walk> }
```

**Note**: we could have implemented this as a bunch Redis strings `walk:<walkID>`.
For fetching a batch of walks, instead of using the built-in `HMGET` we could have done something like

```lua
-- Lua script to retrieve multiple keys
-- KEYS[]: Array of keys to fetch

local results = {}
for i, key in ipairs(KEYS) do
    results[i] = redis.call('GET', key)
end
return results
```

The following table shows the relative speed of this script vs the `HMGET` command:

| batch size | STRINGS (lua script) | HASH (HMGET) |
| ---------- | -------------------- | ------------ |
| 1k         | 15ms                 | 20ms         |
| 10k        | 30ms                 | 30ms         |
| 100k       | 240ms                | 120ms        |

For small batches, the strings are faster, however for big batches the single hash approach is faster, and it's non-blocking.

---

#### walksVisiting

Each `walksVisiting:<nodeID>` (e.g. `walksVisiting:69`, `walksVisiting:420`, ...) is a Redis set containing the IDs of the walks that visit (= contain) `nodeID`.

```
walksVisiting:<nodeID> = SET { <walkID>, <walkID>, ...}
```
---
