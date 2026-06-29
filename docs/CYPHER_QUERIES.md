# Cypher Queries

This document explains the four detection queries in `fraud_detector.py` — what
each one looks for, and why it's written the way it is. The Cypher text below is
copied verbatim from the source; if the code changes, this file should be
updated in the same commit.

## Q1 — Labeled Cycles

Finds fraud cycles using the `cycle_id` label that the synthetic data generator
assigns when it injects a fraud pattern. This is the "ground truth" query: it
only works because we know, from generation time, which transactions are
fraudulent. It exists to validate that the *structural* detection in Q2 (below)
actually finds the same cycles without relying on that label.

```cypher
MATCH (tx:Transaction { label: 'FRAUDE_CICLO' })
WITH tx.cycle_id AS cycle_id,
     sum(tx.amount)    AS total_volume,
     min(tx.timestamp) AS first_at,
     max(tx.timestamp) AS last_at
WHERE cycle_id IS NOT NULL
MATCH (src:BankAccount)-[:SENT]->(t:Transaction { cycle_id: cycle_id })
      -[:RECEIVED]->(dst:BankAccount)
RETURN
    cycle_id,
    count(DISTINCT t)                              AS num_transactions,
    round(total_volume, 2)                         AS total_volume,
    first_at, last_at,
    duration.inSeconds(first_at, last_at).seconds  AS window_seconds,
    collect(DISTINCT src.account_number)           AS source_accounts,
    collect(DISTINCT dst.account_number)           AS dest_accounts
ORDER BY total_volume DESC
```

## Q2 — Structural Cycles (Label-Independent)

This is the query that matters in a real scenario, where transactions don't
come pre-labeled as fraudulent. It looks for the *shape* of a laundering
cycle directly in the graph topology: a path that starts and ends at the same
`BankAccount`, with 2 to 5 hops, completed in under 2 hours (7200 seconds) —
short enough to rule out unrelated transactions that happen to form a
coincidental loop over weeks or months.

```cypher
MATCH path = (start:BankAccount)
      (()-[:SENT]->(:Transaction)-[:RECEIVED]->()){2,5}
      (start)
WITH start,
     [n IN nodes(path) WHERE n:Transaction | n.amount]     AS amounts,
     [n IN nodes(path) WHERE n:Transaction | n.timestamp]  AS timestamps,
     [n IN nodes(path) WHERE n:BankAccount | n.account_number] AS accounts
UNWIND timestamps AS ts
WITH start, amounts, timestamps, accounts,
     min(ts) AS first_at,
     max(ts) AS last_at
WHERE first_at IS NOT NULL
  AND last_at IS NOT NULL
  AND duration.inSeconds(first_at, last_at).seconds < 7200
RETURN DISTINCT
    start.account_number               AS origin_account,
    size(amounts)                      AS cycle_hops,
    round(amounts[0],  2)              AS initial_amount,
    round(amounts[-1], 2)              AS final_amount,
    round(amounts[0] - amounts[-1], 2) AS laundered_fees,
    duration.inSeconds(first_at, last_at).seconds AS cycle_window_seconds,
    accounts                           AS participant_accounts
ORDER BY initial_amount DESC
LIMIT 20
```

**Implementation history (kept here deliberately — this is the kind of detail
that demonstrates the query was debugged against a real database, not written
once and assumed correct):**

1. An earlier version used `length(path)` to filter by hop count. That broke
   after adopting the quantified path pattern syntax —
   `(()-[:SENT]->(:Transaction)-[:RECEIVED]->())+` — which Neo4j 5.9+ does
   not report a `length()` for the way it does for simple paths. The fix was
   to derive the hop count from `size(amounts)` instead.
2. The `+` quantifier itself was unbounded — it told Neo4j to search for
   cycles of *any* length, which is combinatorially expensive. On a
   21-account dataset, this made the query hang indefinitely. It is now
   bounded to `{2,5}`, matching the generator's actual cycle size range —
   the `WHERE size(amounts) >= 2 AND <= 5` filter from the previous version
   became redundant and was removed.
3. The temporal filter originally read `timestamps[0]` and `timestamps[-1]` —
   the first and last `Transaction` timestamps *in path-traversal order*,
   which is not necessarily chronological order. When a path happened to be
   matched "backwards" relative to time, this produced a **negative**
   duration — and any negative number satisfies `< 7200`, so the filter
   passed rows it should have rejected (early test runs showed cycle windows
   of "44 hours" and "295 hours", the visible symptom of this bug). The fix
   unwinds the timestamp list and computes `min(ts)` / `max(ts)` directly,
   which is order-independent.

**Known remaining behavior (not a bug, but worth knowing):** this query
returns the same cycle once per node where Cypher could bind `start` — a
4-hop cycle produces 4 rows, one per rotation. `RETURN DISTINCT` does not
catch this because the rows differ in `origin_account`. Deduplication happens
in Python, in `FraudDetector._detect_structural_cycles()`, by grouping rows
that share the same *set* of `participant_accounts` regardless of rotation.
One side effect: because `initial_amount` and `final_amount` come from
whichever rotation Cypher happened to return, the "fee" calculated as
`initial_amount - final_amount` can come out **negative** for some kept rows
— it does not mean money was created, it means that particular rotation's
amount list doesn't start at the cycle's true chronological origin. Only Q1
(which uses the `cycle_id` label and true `min`/`max` timestamps) reports
fee direction reliably. This is documented as a known limitation in
[`ARCHITECTURE.md`](./ARCHITECTURE.md#5-known-limitations).

## Q3 — Account Risk Ranking

Aggregates, per account, how many fraudulent transactions it sent, the total
suspicious volume, and how many distinct cycles it took part in. This turns
individual flagged transactions into a per-account risk score — the kind of
output a compliance analyst would actually act on ("freeze account X"), rather
than a flat list of suspicious transactions with no account-level context.

```cypher
MATCH (a:BankAccount)-[:SENT]->(tx:Transaction { label: 'FRAUDE_CICLO' })
WITH a,
     count(tx)                    AS fraud_tx_count,
     sum(tx.amount)               AS total_fraud_volume,
     count(DISTINCT tx.cycle_id)  AS cycles_involved
RETURN
    a.account_number              AS account_number,
    a.bank_name                   AS bank,
    fraud_tx_count                AS suspicious_transactions,
    cycles_involved               AS cycles_involved,
    round(total_fraud_volume, 2)  AS total_suspicious_volume
ORDER BY total_suspicious_volume DESC
LIMIT 15
```

## Q4 — Temporal Analysis & Risk Classification

Buckets each labeled cycle into a risk tier based on how fast the money moved
through it. The thresholds encode a simple but defensible heuristic: the
faster a cycle completes, the less plausible it is as a sequence of
independent, legitimate transactions.

```cypher
MATCH (tx:Transaction { label: 'FRAUDE_CICLO' })
WHERE tx.cycle_id IS NOT NULL
WITH tx.cycle_id AS cycle_id,
     min(tx.timestamp) AS first_at,
     max(tx.timestamp) AS last_at,
     count(tx)         AS hops,
     sum(tx.amount)    AS volume
RETURN
    cycle_id, hops,
    round(volume, 2) AS total_volume,
    first_at, last_at,
    duration.inSeconds(first_at, last_at).seconds AS window_seconds,
    CASE
        WHEN duration.inSeconds(first_at, last_at).seconds < 600  THEN 'CRITICAL'
        WHEN duration.inSeconds(first_at, last_at).seconds < 1800 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS risk_level
ORDER BY window_seconds ASC
```

> Note: the `risk_level` values in the live code are currently in Portuguese
> (`'CRÍTICO'`, `'ALTO'`, `'MÉDIO'`) — translating them to English (as shown
> above) is one of the items tracked for the technical review pass, to keep
> all string literals consistent with the project's English-identifiers
> convention.