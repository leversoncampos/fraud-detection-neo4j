"""
Módulo 2 — Pipeline de Ingestão Neo4j
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import asdict
from typing import Any, Generator

from neo4j import GraphDatabase, ManagedTransaction, Session
from neo4j.exceptions import AuthError, ServiceUnavailable, Neo4jError

from data_generator import BankAccount, Client, SyntheticDataset, Transaction

logger = logging.getLogger(__name__)

# ── Schema (IF NOT EXISTS = idempotente) ──────────────────────────
_CONSTRAINT_CLIENT = """
    CREATE CONSTRAINT client_id_unique IF NOT EXISTS
    FOR (c:Client) REQUIRE c.client_id IS UNIQUE
"""
_CONSTRAINT_ACCOUNT = """
    CREATE CONSTRAINT account_id_unique IF NOT EXISTS
    FOR (a:BankAccount) REQUIRE a.account_id IS UNIQUE
"""
_CONSTRAINT_TX = """
    CREATE CONSTRAINT tx_id_unique IF NOT EXISTS
    FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE
"""
_INDEX_TX_LABEL = """
    CREATE INDEX tx_label_idx IF NOT EXISTS
    FOR (t:Transaction) ON (t.label)
"""
_INDEX_TX_CYCLE = """
    CREATE INDEX tx_cycle_idx IF NOT EXISTS
    FOR (t:Transaction) ON (t.cycle_id)
"""

# ── Queries de ingestão (UNWIND + MERGE = idempotente e eficiente) ─
_MERGE_CLIENTS = """
    UNWIND $batch AS row
    MERGE (c:Client { client_id: row.client_id })
    SET c.name  = row.name,
        c.cpf   = row.cpf,
        c.email = row.email
"""

_MERGE_ACCOUNTS = """
    UNWIND $batch AS row
    MERGE (a:BankAccount { account_id: row.account_id })
    SET a.account_number = row.account_number,
        a.balance        = row.balance,
        a.bank_name      = row.bank_name
    WITH a, row
    MATCH (c:Client { client_id: row.client_id })
    MERGE (c)-[:HAS_ACCOUNT]->(a)
"""

_MERGE_TRANSACTIONS = """
    UNWIND $batch AS row
    MERGE (tx:Transaction { tx_id: row.tx_id })
    SET tx.amount    = row.amount,
        tx.timestamp = row.timestamp,
        tx.label     = row.label,
        tx.cycle_id  = row.cycle_id
    WITH tx, row
    MATCH (src:BankAccount { account_id: row.from_account_id })
    MATCH (dst:BankAccount { account_id: row.to_account_id })
    MERGE (src)-[:SENT]->(tx)
    MERGE (tx)-[:RECEIVED]->(dst)
"""


def _chunked(items: list[Any], size: int) -> Generator[list[Any], None, None]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class Neo4jPipeline:
    """
    Gerencia ingestão completa no Neo4j.

    Uso:
        with Neo4jPipeline(uri, user, password) as p:
            p.setup_schema()
            p.ingest(dataset)
    """

    BATCH_SIZE: int = 500

    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j") -> None:
        self._uri      = uri
        self._user     = user
        self._password = password
        self._database = database
        self._driver   = None

    def __enter__(self) -> "Neo4jPipeline":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def connect(self) -> None:
        try:
            self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
            self._driver.verify_connectivity()
            logger.info("✅  Conectado ao Neo4j em %s", self._uri)
        except ServiceUnavailable:
            logger.error("❌  Neo4j inacessível — verifique se está rodando.")
            raise
        except AuthError:
            logger.error("❌  Credenciais inválidas para '%s'.", self._user)
            raise

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            logger.info("🔌  Conexão encerrada.")

    def setup_schema(self) -> None:
        """Cria constraints e índices. Idempotente."""
        schema = [
            ("Constraint Client",      _CONSTRAINT_CLIENT),
            ("Constraint BankAccount", _CONSTRAINT_ACCOUNT),
            ("Constraint Transaction", _CONSTRAINT_TX),
            ("Index tx.label",         _INDEX_TX_LABEL),
            ("Index tx.cycle_id",      _INDEX_TX_CYCLE),
        ]
        with self._session() as session:
            for name, stmt in schema:
                session.run(stmt)
                logger.debug("    ✔ %s criado/verificado.", name)
        logger.info("✅  Schema configurado.")

    def ingest(self, dataset: SyntheticDataset) -> None:
        logger.info("🚀  Iniciando ingestão…")
        self._ingest_clients(dataset.clients)
        self._ingest_accounts(dataset.accounts)
        self._ingest_transactions(dataset.transactions)
        logger.info("✅  Ingestão concluída.")

    def clear_database(self) -> None:
        logger.warning("⚠️   Limpando banco de dados!")
        with self._session() as session:
            session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        logger.info("🗑   Banco limpo.")

    def _ingest_clients(self, clients: list[Client]) -> None:
        logger.info("  👤  Inserindo %d clientes…", len(clients))
        data = [asdict(c) for c in clients]
        with self._session() as session:
            for chunk in _chunked(data, self.BATCH_SIZE):
                session.execute_write(self._write_batch, _MERGE_CLIENTS, chunk)

    def _ingest_accounts(self, accounts: list[BankAccount]) -> None:
        logger.info("  🏦  Inserindo %d contas…", len(accounts))
        data = [asdict(a) for a in accounts]
        with self._session() as session:
            for chunk in _chunked(data, self.BATCH_SIZE):
                session.execute_write(self._write_batch, _MERGE_ACCOUNTS, chunk)

    def _ingest_transactions(self, transactions: list[Transaction]) -> None:
        logger.info("  💸  Inserindo %d transações…", len(transactions))
        data = [asdict(t) for t in transactions]
        with self._session() as session:
            for chunk in _chunked(data, self.BATCH_SIZE):
                session.execute_write(self._write_batch, _MERGE_TRANSACTIONS, chunk)

    @staticmethod
    def _write_batch(tx: ManagedTransaction, query: str, batch: list[dict[str, Any]]) -> None:
        tx.run(query, batch=batch)

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        if self._driver is None:
            raise RuntimeError("Driver não inicializado.")
        session = self._driver.session(database=self._database)
        try:
            yield session
        except Neo4jError as exc:
            logger.error("❌  Erro Neo4j: %s", exc.message)
            raise
        finally:
            session.close()
