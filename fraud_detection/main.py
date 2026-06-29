"""
Main orchestrator.

Usage:
    python main.py                   # full pipeline
    python main.py --skip-ingest     # detection only (database already populated)
    python main.py --clear           # clear database before inserting
    python main.py --seed 99         # different data
    python main.py --clients 50 --fraud-cycles 5
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from data_generator import SyntheticDataGenerator
from fraud_detector import FraudDetector
from neo4j_pipeline import Neo4jPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


def _load_config() -> dict:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info("📄  Configuration loaded from: %s", env_path)
    else:
        logger.warning("⚠️   .env not found — using system environment variables.")

    missing = [k for k in ("NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD") if not os.getenv(k)]
    if missing:
        logger.error(
            "❌  Missing variables: %s\n    → Copy .env.example to .env and fill it in.",
            ", ".join(missing),
        )
        sys.exit(1)

    return {
        "uri":              os.environ["NEO4J_URI"],
        "user":             os.environ["NEO4J_USER"],
        "password":         os.environ["NEO4J_PASSWORD"],
        "database":         os.getenv("NEO4J_DATABASE",      "neo4j"),
        "num_clients":      int(os.getenv("NUM_CLIENTS",      "20")),
        "num_transactions": int(os.getenv("NUM_TRANSACTIONS", "40")),
        "num_fraud_cycles": int(os.getenv("NUM_FRAUD_CYCLES", "3")),
        "random_seed":      int(os.getenv("RANDOM_SEED",      "42")),
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fraud Detection — Neo4j")
    p.add_argument("--skip-ingest",  action="store_true",
                   help="Skip generation/ingestion (database already populated).")
    p.add_argument("--clear",        action="store_true",
                   help="Clear the database before inserting.")
    p.add_argument("--seed",         type=int, default=None,
                   help="Overrides RANDOM_SEED.")
    p.add_argument("--clients",      type=int, default=None,
                   help="Overrides NUM_CLIENTS.")
    p.add_argument("--fraud-cycles", type=int, default=None,
                   help="Overrides NUM_FRAUD_CYCLES.")
    return p.parse_args()


def main() -> None:
    args   = _parse_args()
    config = _load_config()

    print("\n" + "═" * 56)
    print("  FRAUD DETECTION — Neo4j · Cyclical Smurfing")
    print("═" * 56)

    if not args.skip_ingest:
        seed    = args.seed         or config["random_seed"]
        clients = args.clients      or config["num_clients"]
        cycles  = args.fraud_cycles or config["num_fraud_cycles"]

        logger.info("STEP 1 — Generating synthetic data…")
        gen     = SyntheticDataGenerator(seed=seed)
        dataset = gen.generate(
            num_clients=clients,
            num_transactions=config["num_transactions"],
            num_fraud_cycles=cycles,
        )
        print(dataset.summary())

        logger.info("STEP 2 — Ingesting into Neo4j…")
        with Neo4jPipeline(
            uri=config["uri"], user=config["user"],
            password=config["password"], database=config["database"],
        ) as pipeline:
            if args.clear:
                pipeline.clear_database()
            pipeline.setup_schema()
            pipeline.ingest(dataset)
    else:
        logger.info("⏭   Ingestion skipped (--skip-ingest).")

    logger.info("STEP 3 — Running fraud detection…")
    with FraudDetector(
        uri=config["uri"], user=config["user"],
        password=config["password"], database=config["database"],
    ) as detector:
        report = detector.run_full_detection()
        detector.print_report(report)


if __name__ == "__main__":
    main()