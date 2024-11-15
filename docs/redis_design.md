# Redis Design Document

These are the data structures we decided to use:

## Database Interface

The following data structures are used to implement the Database Interface using Redis as the database.

#### database

The `database` is a Redis hash that encapsulate relevant metadata about the database.
The field `lastNodeID` is used to assign unique IDs to new nodes.

```
database = HASH { lastNodeID: <nodeID>, <some-statistic>: <stats>}
```

---

#### keyIndex

The `keyIndex` is a Redis hash that maps each pubkey to a nodeID. This allows to reference nodes by their ID, saving memory (especially for  the sets of `follows` and `followers`)

```
keyIndex = HASH { <pubkey>: <nodeID> }
```

---

#### node

Each `node:<nodeID>` (e.g. `node:69`, `node:420`, ...) is a Redis hash, encapsulating relevant (one-dimentional) data about a node.

```
node:<nodeID> = HASH {    pubkey: <pubkey>
                          timestamp: <timestamp of last kind3>
                          status: <we do random walks/ we don't>
                          pagerank: <pagerank>
                      }
```

---

#### follows

Each `follows:<nodeID>` (e.g. `follows:69`, `follows:420`, ...) is a Redis set containing the IDs of the follows of `nodeID`.

```
follows:<nodeID> = SET { <nodeID>, <nodeID>, ...}
```

---

#### followers

Each `followers:<nodeID>` (e.g. `followers:69`, `followers:420`, ...) is a Redis set containing the IDs of the followers of `nodeID`.

```
followers:<nodeID> = SET {<nodeID>, <nodeID>, ...}
```

---

## RandomWalkStore Interface

The following data structures are used to implement the RandomWalkStore interface using Redis as the in-memory datastore.

#### RWS

The `RWS` is a Redis hash that encapsulate relevant metadata about the RandomWalkStore. The field `lastWalkID` is used to assign unique IDs to new walks.

```
RWS = HASH {     alpha: <alpha>,
                 walksPerNode: <walksPerNode>,
                 lastWalkID: <walkID> 
            }
```

---

#### walks

The `walks` is a Redis hash that maps each walkID to a walk, which is a strings of nodeIDs separated by commas e.g.  `"0,1,2,3,4"`.

```
walks = HASH { <walkID>: <walk> }
```

**Note**: we could have implemented this as a bunch Redis strings `walk:<walkID>`.
For fetching a batch of walks, instead of using the built-in `HMGET` we should have do something like

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

For small batches, the strings are faster, however for big batches the single hash approach is faster.

---

#### walksVisiting

Each `walksVisiting:<nodeID>` (e.g. `walksVisiting:69`, `walksVisiting:420`, ...) is a Redis set containing the IDs of the walks that visit (= contain) `nodeID`.

```
walksVisiting:<nodeID> = SET { <walkID>, <walkID>, ...}
```

---


