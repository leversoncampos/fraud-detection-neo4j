# Methodology

This document explains how `data_generator.py` builds its synthetic dataset,
and why the fraud pattern it injects is a reasonable model of a real
money-laundering technique — not an arbitrary made-up shape.

## The Pattern: Cyclical Layering ("Smurfing")

The generator injects closed transaction loops: money leaves account A, passes
through 3 to 5 intermediate accounts, and returns to A. This is a simplified
model of **layering** (sometimes called *smurfing*), a well-documented
money-laundering technique where funds are moved through a chain of accounts —
often belonging to intermediaries who take a cut — to obscure the funds'
origin before they re-enter the formal economy.

Two properties of real layering schemes are deliberately reproduced:

**1. The amount shrinks slightly at every hop.**
```python
fee = amount * self._rng.uniform(0.005, 0.02)
amount = round(amount - fee, 2)
```
Each intermediary takes between 0.5% and 2% before passing the funds along —
modeling the real-world economics of paying "mules" to move money, rather than
transferring the exact same amount at every hop, which would be an unrealistic
simplification.

**2. The cycle completes in a short, tight time window.**
```python
current_dt += timedelta(minutes=self._rng.randint(FRAUD_WINDOW_MIN, FRAUD_WINDOW_MAX))
```
Each hop happens 2 to 30 minutes after the previous one. Legitimate circular
fund movement (e.g., a small business paying a supplier who happens to be a
customer) tends to be spread over days or weeks; a cycle that completes within
roughly an hour is a much stronger structural signal, and is exactly the
property that [`CYPHER_QUERIES.md`](./CYPHER_QUERIES.md)'s Q2 filters on
(`< 7200` seconds).

## Why Synthetic Data, Not a Public Dataset

Real, labeled money-laundering transaction data is not publicly available —
for obvious legal and privacy reasons, financial institutions don't release
this. Public fraud datasets that do exist (e.g., credit card fraud datasets on
Kaggle) are typically flat/tabular and don't contain the *relational*
structure — the actual chain of transfers between accounts — that this project
is specifically about detecting. Generating synthetic data with a known,
labeled ground truth (the `FRAUDE_CICLO` label) is what makes it possible to
validate Q2's *structural* detection against Q1's *labeled* detection: if both
queries find the same cycles, that's evidence the structural approach works
without relying on information a real-world detector wouldn't have.

## Parameters and Reproducibility

All generation is seeded (`RANDOM_SEED`, default `42`), so the exact same
dataset is produced on every run unless the seed is changed. This matters for
a portfolio project specifically: anyone reviewing this repository should be
able to reproduce the exact numbers shown in any example output, not just a
similar-looking one.

| Parameter | Default | Meaning |
|---|---|---|
| `NUM_CLIENTS` | 20 | Number of synthetic clients generated |
| `NUM_TRANSACTIONS` | 40 | Number of normal (non-fraudulent) transactions |
| `NUM_FRAUD_CYCLES` | 3 | Number of injected laundering cycles |
| `RANDOM_SEED` | 42 | Seed for both `random` and `Faker` |

## Known Simplification

Real laundering schemes are rarely a clean closed loop back to the origin
account — money more often flows onward to a final destination ("integration"
stage) rather than cycling back. The closed-loop shape was chosen here because
it makes the *structural* signal (Q2) unambiguous and independently verifiable
against the *labeled* signal (Q1), which is the comparison this project is
built to demonstrate. A more realistic open-chain layering pattern is a
natural extension, not implemented here — see the architecture document's
limitations section.