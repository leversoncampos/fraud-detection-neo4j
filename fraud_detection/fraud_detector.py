"""
Módulo 3 — Detector de Fraude
Executa 4 queries de detecção e imprime relatório formatado.

CORREÇÃO: Q2 — removido length(path) incompatível com quantified path patterns
do Neo4j 5. O filtro de número de saltos agora usa size(amounts) após o WITH.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generator

from neo4j import GraphDatabase, Session
from neo4j.exceptions import Neo4jError

logger = logging.getLogger(__name__)


class _C:
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    GREEN  = "\033[92m"
    CYAN   = "\033[96m"
    BLUE   = "\033[94m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RESET  = "\033[0m"


# ── Q1 — Ciclos rotulados ─────────────────────────────────────────
_Q1_LABELED_CYCLES = """
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
"""

# ── Q2 — Ciclos estruturais (sem labels) ─────────────────────────
# CORREÇÃO: length(path) foi removido.
# - Quantified path patterns (Neo4j 5.9+) não suportam length()
# - O número de saltos é inferido via size(amounts) após o WITH
# - Janela temporal < 2h (7200s) filtra rotas coincidentes legítimas
_Q2_STRUCTURAL_CYCLES = """
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
"""

# ── Q3 — Ranking de contas por volume suspeito ───────────────────
_Q3_ACCOUNT_RISK_RANKING = """
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
"""

# ── Q4 — Análise temporal com classificação de risco ─────────────
_Q4_TEMPORAL_ANALYSIS = """
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
"""


@dataclass
class FraudCycleResult:
    cycle_id: str
    num_transactions: int
    total_volume: float
    started_at: datetime | None
    ended_at: datetime | None
    window_seconds: int
    source_accounts: list[str]
    dest_accounts: list[str]


@dataclass
class AccountRiskResult:
    account_number: str
    bank: str
    suspicious_transactions: int
    cycles_involved: int
    total_suspicious_volume: float


@dataclass
class DetectionReport:
    labeled_cycles: list[FraudCycleResult]
    structural_cycles: list[dict[str, Any]]
    account_risks: list[AccountRiskResult]
    temporal_analysis: list[dict[str, Any]]


class FraudDetector:
    """
    Executa detecção contra banco Neo4j já populado.

    Uso:
        with FraudDetector(uri, user, password) as d:
            report = d.run_full_detection()
            d.print_report(report)
    """

    def __init__(self, uri, user, password, database="neo4j") -> None:
        self._uri      = uri
        self._user     = user
        self._password = password
        self._database = database
        self._driver   = None

    def __enter__(self) -> "FraudDetector":
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
        self._driver.verify_connectivity()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._driver:
            self._driver.close()

    def run_full_detection(self) -> DetectionReport:
        with self._session() as session:
            return DetectionReport(
                labeled_cycles    = self._detect_labeled_cycles(session),
                structural_cycles = self._detect_structural_cycles(session),
                account_risks     = self._rank_account_risk(session),
                temporal_analysis = self._temporal_analysis(session),
            )

    def print_report(self, report: DetectionReport) -> None:
        self._print_header()
        self._print_labeled_cycles(report.labeled_cycles)
        self._print_structural_cycles(report.structural_cycles)
        self._print_account_risk(report.account_risks)
        self._print_temporal(report.temporal_analysis)
        self._print_footer(report)

    def _detect_labeled_cycles(self, session: Session) -> list[FraudCycleResult]:
        return [
            FraudCycleResult(
                cycle_id         = r["cycle_id"],
                num_transactions = r["num_transactions"],
                total_volume     = r["total_volume"],
                started_at       = r["first_at"],
                ended_at         = r["last_at"],
                window_seconds   = r["window_seconds"] or 0,
                source_accounts  = list(r["source_accounts"]),
                dest_accounts    = list(r["dest_accounts"]),
            )
            for r in session.run(_Q1_LABELED_CYCLES)
        ]

    def _detect_structural_cycles(self, session: Session) -> list[dict]:
        return [dict(r) for r in session.run(_Q2_STRUCTURAL_CYCLES)]

    def _rank_account_risk(self, session: Session) -> list[AccountRiskResult]:
        return [
            AccountRiskResult(
                account_number          = r["account_number"],
                bank                    = r["bank"],
                suspicious_transactions = r["suspicious_transactions"],
                cycles_involved         = r["cycles_involved"],
                total_suspicious_volume = r["total_suspicious_volume"],
            )
            for r in session.run(_Q3_ACCOUNT_RISK_RANKING)
        ]

    def _temporal_analysis(self, session: Session) -> list[dict]:
        return [dict(r) for r in session.run(_Q4_TEMPORAL_ANALYSIS)]

    @staticmethod
    def _fmt_seconds(s: int) -> str:
        m, s = divmod(abs(s), 60)
        h, m = divmod(m, 60)
        if h: return f"{h}h {m}min {s}s"
        if m: return f"{m}min {s}s"
        return f"{s}s"

    @staticmethod
    def _fmt_brl(v: float) -> str:
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def _print_header(self) -> None:
        w = 68
        print(f"\n{_C.BOLD}{_C.CYAN}{'═' * w}")
        print(f"  {'DETECÇÃO DE FRAUDE FINANCEIRA — Neo4j':^64}")
        print(f"  {'Lavagem de Dinheiro em Ciclo (Smurfing)':^64}")
        print(f"{'═' * w}{_C.RESET}\n")

    def _print_labeled_cycles(self, cycles: list[FraudCycleResult]) -> None:
        print(f"{_C.BOLD}{_C.RED}{'─' * 68}")
        print("  ⚠  Q1 — CICLOS ROTULADOS (FRAUDE_CICLO)")
        print(f"{'─' * 68}{_C.RESET}")
        if not cycles:
            print(f"  {_C.GREEN}Nenhum ciclo encontrado.{_C.RESET}\n")
            return
        for i, c in enumerate(cycles, 1):
            contas = sorted(set(c.source_accounts) | set(c.dest_accounts))
            print(f"\n  {_C.BOLD}{_C.RED}CICLO #{i}{_C.RESET}  "
                  f"{_C.DIM}(id: {c.cycle_id[:8]}…){_C.RESET}")
            print(f"  {'Transações':<26}: {_C.YELLOW}{c.num_transactions}{_C.RESET}")
            print(f"  {'Volume total lavado':<26}: {_C.RED}{self._fmt_brl(c.total_volume)}{_C.RESET}")
            print(f"  {'Janela temporal':<26}: {_C.YELLOW}{self._fmt_seconds(c.window_seconds)}{_C.RESET}")
            print(f"  {'Início':<26}: {c.started_at}")
            print(f"  {'Fim':<26}: {c.ended_at}")
            print(f"  {'Contas participantes':<26}:")
            for acc in contas:
                print(f"    {_C.RED}▶  {acc}{_C.RESET}")
        print()

    def _print_structural_cycles(self, cycles: list[dict]) -> None:
        print(f"{_C.BOLD}{_C.YELLOW}{'─' * 68}")
        print("  🔎  Q2 — CICLOS ESTRUTURAIS (sem depender de labels)")
        print(f"{'─' * 68}{_C.RESET}")
        if not cycles:
            print(f"  {_C.GREEN}Nenhum ciclo estrutural.{_C.RESET}\n")
            return
        for i, c in enumerate(cycles, 1):
            print(f"\n  {_C.BOLD}CICLO #{i}{_C.RESET}")
            print(f"  {'Conta de origem':<26}: {_C.YELLOW}{c.get('origin_account')}{_C.RESET}")
            print(f"  {'Saltos':<26}: {c.get('cycle_hops')}")
            print(f"  {'Valor inicial':<26}: {self._fmt_brl(c.get('initial_amount', 0))}")
            print(f"  {'Valor final':<26}: {self._fmt_brl(c.get('final_amount', 0))}")
            print(f"  {'Taxas (laranjas)':<26}: {_C.RED}{self._fmt_brl(c.get('laundered_fees', 0))}{_C.RESET}")
            print(f"  {'Janela':<26}: {_C.YELLOW}{self._fmt_seconds(c.get('cycle_window_seconds', 0))}{_C.RESET}")
            accs = c.get("participant_accounts", [])
            print(f"  {'Cadeia':<26}: {_C.YELLOW}{' → '.join(str(a) for a in accs)}{_C.RESET}")
        print()

    def _print_account_risk(self, risks: list[AccountRiskResult]) -> None:
        print(f"{_C.BOLD}{_C.BLUE}{'─' * 68}")
        print("  📊  Q3 — RANKING DE CONTAS SUSPEITAS")
        print(f"{'─' * 68}{_C.RESET}")
        if not risks:
            print(f"  {_C.GREEN}Sem contas em risco.{_C.RESET}\n")
            return
        print(f"  {_C.DIM}{'#':<4}{'Conta':<18}{'Banco':<20}{'TXs':>6}{'Ciclos':>8}{'Volume':>18}{_C.RESET}")
        print(f"  {'─' * 64}")
        for i, r in enumerate(risks, 1):
            color = _C.RED if i <= 3 else _C.YELLOW
            print(
                f"  {color}{_C.BOLD}{i:<4}{_C.RESET}"
                f"{r.account_number:<18}{r.bank:<20}"
                f"{_C.YELLOW}{r.suspicious_transactions:>6}{_C.RESET}"
                f"{r.cycles_involved:>8}"
                f"  {_C.RED}{self._fmt_brl(r.total_suspicious_volume):>16}{_C.RESET}"
            )
        print()

    def _print_temporal(self, temporal: list[dict]) -> None:
        print(f"{_C.BOLD}{_C.CYAN}{'─' * 68}")
        print("  ⏱   Q4 — ANÁLISE TEMPORAL E CLASSIFICAÇÃO DE RISCO")
        print(f"{'─' * 68}{_C.RESET}")
        colors = {"CRITICAL": _C.RED, "HIGH": _C.YELLOW, "MEDIUM": _C.BLUE}
        for r in temporal:
            col   = colors.get(str(r.get("risk_level", "")), _C.RESET)
            level = r.get("risk_level", "—")
            print(
                f"  {col}[{level:^8}]{_C.RESET}"
                f"  {str(r.get('cycle_id',''))[:8]}…"
                f"  |  {r.get('hops')} saltos"
                f"  |  {self._fmt_seconds(r.get('window_seconds', 0))}"
                f"  |  {self._fmt_brl(r.get('total_volume', 0))}"
            )
        print()

    def _print_footer(self, report: DetectionReport) -> None:
        vol = sum(c.total_volume for c in report.labeled_cycles)
        print(f"{_C.BOLD}{_C.CYAN}{'═' * 68}")
        print("  RESUMO EXECUTIVO")
        print(f"{'─' * 68}{_C.RESET}")
        print(f"  {'Ciclos rotulados':<38}: {_C.RED}{len(report.labeled_cycles)}{_C.RESET}")
        print(f"  {'Ciclos estruturais':<38}: {_C.YELLOW}{len(report.structural_cycles)}{_C.RESET}")
        print(f"  {'Contas em risco':<38}: {_C.YELLOW}{len(report.account_risks)}{_C.RESET}")
        print(f"  {'Volume total suspeito':<38}: {_C.RED}{self._fmt_brl(vol)}{_C.RESET}")
        print(f"{_C.BOLD}{_C.CYAN}{'═' * 68}{_C.RESET}\n")

    @contextmanager
    def _session(self) -> Generator[Session, None, None]:
        session = self._driver.session(database=self._database)
        try:
            yield session
        except Neo4jError as exc:
            logger.error("Erro Neo4j: %s", exc.message)
            raise
        finally:
            session.close()
