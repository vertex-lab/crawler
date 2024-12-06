# Database Design

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
node:<nodeID> = HASH {    id: <nodeID>
                          pubkey: <pubkey>
                          event_timestamp: <timestamp of last kind3>
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