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
      (()-[:SENT]->(:Transaction)-[:RECEIVED]->())+
      (start)
WITH start,
     [n IN nodes(path) WHERE n:Transaction | n.amount]     AS amounts,
     [n IN nodes(path) WHERE n:Transaction | n.timestamp]  AS timestamps,
     [n IN nodes(path) WHERE n:BankAccount | n.account_number] AS accounts
WHERE size(amounts) >= 2
  AND size(amounts) <= 5
  AND timestamps[0] IS NOT NULL
  AND timestamps[-1] IS NOT NULL
  AND duration.inSeconds(timestamps[0], timestamps[-1]).seconds < 7200
RETURN DISTINCT
    start.account_number               AS origin_account,
    size(amounts)                      AS cycle_hops,
    round(amounts[0],  2)              AS initial_amount,
    round(amounts[-1], 2)              AS final_amount,
    round(amounts[0] - amounts[-1], 2) AS laundered_fees,
    duration.inSeconds(
        timestamps[0], timestamps[-1]
    ).seconds                          AS cycle_window_seconds,
    accounts                           AS participant_accounts
ORDER BY initial_amount DESC
LIMIT 20
```

**Implementation note (kept here deliberately, not just as a code comment):**
an earlier version of this query used `length(path)` to filter by the number of
hops. That stopped working after upgrading to a quantified path pattern syntax
— `(()-[:SENT]->(:Transaction)-[:RECEIVED]->())+` — which Neo4j 5.9+ does not
report a `length()` for in the way older single-relationship paths do. The fix
was to derive the hop count from `size(amounts)` instead, computed *after* the
`WITH` clause, since `amounts` is collected directly from the path's
`Transaction` nodes regardless of how the path itself was matched.

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