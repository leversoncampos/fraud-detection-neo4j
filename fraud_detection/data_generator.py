"""
Módulo 1 — Gerador de Dados Sintéticos
Gera clientes, contas e transações com padrão de fraude em ciclo.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Final

from faker import Faker

FRAUD_LABEL: Final[str] = "FRAUDE_CICLO"
NORMAL_LABEL: Final[str] = "NORMAL"

MIN_BALANCE: Final[float] = 1_000.0
MAX_BALANCE: Final[float] = 500_000.0
MIN_TX_VALUE: Final[float] = 50.0
MAX_TX_VALUE: Final[float] = 15_000.0

FRAUD_WINDOW_MIN: Final[int] = 2
FRAUD_WINDOW_MAX: Final[int] = 30


@dataclass(frozen=True)
class Client:
    client_id: str
    name: str
    cpf: str
    email: str


@dataclass(frozen=True)
class BankAccount:
    account_id: str
    account_number: str
    balance: float
    client_id: str
    bank_name: str


@dataclass(frozen=True)
class Transaction:
    tx_id: str
    from_account_id: str
    to_account_id: str
    amount: float
    timestamp: datetime
    label: str
    cycle_id: str | None


@dataclass
class SyntheticDataset:
    clients: list[Client] = field(default_factory=list)
    accounts: list[BankAccount] = field(default_factory=list)
    transactions: list[Transaction] = field(default_factory=list)

    @property
    def fraud_transactions(self) -> list[Transaction]:
        return [t for t in self.transactions if t.label == FRAUD_LABEL]

    @property
    def normal_transactions(self) -> list[Transaction]:
        return [t for t in self.transactions if t.label == NORMAL_LABEL]

    def summary(self) -> str:
        cycles = len({t.cycle_id for t in self.fraud_transactions if t.cycle_id})
        return (
            f"\n{'─' * 50}\n"
            f"  DATASET GERADO\n"
            f"{'─' * 50}\n"
            f"  Clientes           : {len(self.clients):>5}\n"
            f"  Contas bancárias   : {len(self.accounts):>5}\n"
            f"  Transações normais : {len(self.normal_transactions):>5}\n"
            f"  Transações fraude  : {len(self.fraud_transactions):>5}\n"
            f"  Ciclos de fraude   : {cycles:>5}\n"
            f"{'─' * 50}\n"
        )


class SyntheticDataGenerator:
    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._fake = Faker("pt_BR")
        self._fake.seed_instance(seed)
        Faker.seed(seed)

    def generate(self, num_clients=20, num_transactions=40, num_fraud_cycles=3) -> SyntheticDataset:
        if num_clients < 6:
            raise ValueError("Mínimo de 6 clientes.")
        clients  = self._generate_clients(num_clients)
        accounts = self._generate_accounts(clients)
        dataset  = SyntheticDataset(clients=clients, accounts=accounts)
        self._inject_normal_transactions(dataset, num_transactions)
        self._inject_fraud_cycles(dataset, num_fraud_cycles)
        return dataset

    def _generate_clients(self, count):
        clients, used_cpfs = [], set()
        while len(clients) < count:
            cpf = self._fake.cpf()
            if cpf in used_cpfs: continue
            used_cpfs.add(cpf)
            clients.append(Client(str(uuid.uuid4()), self._fake.name(), cpf, self._fake.email()))
        return clients

    def _generate_accounts(self, clients):
        banks = ["Banco do Brasil","Caixa Econômica","Itaú Unibanco","Bradesco","Santander","Nubank","Inter"]
        return [BankAccount(str(uuid.uuid4()), self._fake.bban(),
                            round(self._rng.uniform(MIN_BALANCE, MAX_BALANCE), 2),
                            c.client_id, self._rng.choice(banks)) for c in clients]

    def _inject_normal_transactions(self, dataset, count):
        base_dt = datetime.now()
        for _ in range(count):
            src, dst = self._rng.sample(dataset.accounts, k=2)
            dataset.transactions.append(Transaction(
                str(uuid.uuid4()), src.account_id, dst.account_id,
                round(self._rng.uniform(MIN_TX_VALUE, MAX_TX_VALUE), 2),
                base_dt - timedelta(days=self._rng.randint(0,30), hours=self._rng.randint(0,23), minutes=self._rng.randint(0,59)),
                NORMAL_LABEL, None))

    def _inject_fraud_cycles(self, dataset, num_cycles):
        for _ in range(num_cycles):
            cycle_id   = str(uuid.uuid4())
            cycle_size = self._rng.choice([3,4,5])
            if len(dataset.accounts) < cycle_size: break
            participants = self._rng.sample(dataset.accounts, k=cycle_size)
            amount       = round(self._rng.uniform(5_000.0, 50_000.0), 2)
            current_dt   = datetime.now() - timedelta(days=self._rng.randint(1,7))
            for hop in range(cycle_size):
                src = participants[hop]
                dst = participants[(hop + 1) % cycle_size]
                fee = amount * self._rng.uniform(0.005, 0.02)
                amount = round(amount - fee, 2)
                current_dt += timedelta(minutes=self._rng.randint(FRAUD_WINDOW_MIN, FRAUD_WINDOW_MAX))
                dataset.transactions.append(Transaction(
                    str(uuid.uuid4()), src.account_id, dst.account_id,
                    amount, current_dt, FRAUD_LABEL, cycle_id))
