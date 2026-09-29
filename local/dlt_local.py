"""Runs the pipeline SQL files on open-source Spark, so the transformation logic
can be tested on a laptop and in CI without a Databricks workspace.

This is a test harness, not a Lakeflow implementation. It executes the same
SQL text that deploys to Databricks and translates only the pipeline-specific
wrappers:

  CREATE OR REFRESH STREAMING TABLE / MATERIALIZED VIEW x (...) AS <query>
      -> <query> is run in batch and registered as view x
  CONSTRAINT c EXPECT (e) ON VIOLATION DROP ROW
      -> rows where e is not true are removed and counted
  CONSTRAINT c EXPECT (e)                 (warn)  -> counted only
  CONSTRAINT c EXPECT (e) ON VIOLATION FAIL UPDATE -> raises
  STREAM(x) / STREAM x                    -> x
  read_files(...)                         -> the landing folder, read as text
  APPLY CHANGES INTO ... STORED AS SCD TYPE 1 | 2
      -> window-function emulation of Databricks CDC semantics

Everything else (the SELECT logic, joins, windows, JSON parsing) runs exactly
as written. Spark runs in ANSI mode, like Databricks SQL, so casts and
divisions that would fail in production fail here too.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipelines"

CREATE_RE = re.compile(
    r"^CREATE\s+OR\s+REFRESH\s+(?:PRIVATE\s+)?(STREAMING\s+TABLE|MATERIALIZED\s+VIEW)\s+(\w+)\s*(.*)$",
    re.IGNORECASE | re.DOTALL,
)
CONSTRAINT_RE = re.compile(
    r"CONSTRAINT\s+(\w+)\s+EXPECT\s*\((.*)\)\s*(ON\s+VIOLATION\s+(DROP\s+ROW|FAIL\s+UPDATE))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
APPLY_RE = re.compile(
    r"^APPLY\s+CHANGES\s+INTO\s+(\w+)\s+FROM\s+STREAM\s*\(\s*(\w+)\s*\)\s+KEYS\s*\(([^)]*)\)"
    r"(?:\s+APPLY\s+AS\s+DELETE\s+WHEN\s+(.*?))?"
    r"\s+SEQUENCE\s+BY\s+(.*?)"
    r"(?:\s+COLUMNS\s+\*\s+EXCEPT\s*\(([^)]*)\))?"
    r"\s+STORED\s+AS\s+SCD\s+TYPE\s+([12])"
    r"(?:\s+TRACK\s+HISTORY\s+ON\s+\*\s+EXCEPT\s*\(([^)]*)\))?\s*$",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class RunResult:
    tables: list[str] = field(default_factory=list)
    # (table, constraint) -> number of rows that failed it
    expectation_failures: dict[tuple[str, str], int] = field(default_factory=dict)


def local_spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("clinical-ops-local")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _strip_comments(sql: str) -> str:
    return "\n".join(re.sub(r"--.*$", "", line) for line in sql.splitlines())


def _split_statements(sql: str) -> list[str]:
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", _strip_comments(sql)) if s.strip()]


def _take_parenthesized(text: str) -> tuple[str, str]:
    """text starts with '('. Returns (inside, rest) using balanced parentheses,
    ignoring parentheses inside quotes."""
    depth, quote = 0, None
    for i, ch in enumerate(text):
        if quote:
            if ch == quote:
                quote = None
            continue
        if ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[1:i], text[i + 1 :]
    raise ValueError("unbalanced parentheses")


def _split_top_level_commas(text: str) -> list[str]:
    parts, depth, quote, cur = [], 0, None, []
    for ch in text:
        if quote:
            quote = None if ch == quote else quote
        elif ch in "'\"":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def _strip_stream(query: str) -> str:
    query = re.sub(r"\bSTREAM\s*\(\s*(\w+)\s*\)", r"\1", query, flags=re.IGNORECASE)
    return re.sub(r"\bSTREAM\s+(\w+)\b(?!\s*\()", r"\1", query, flags=re.IGNORECASE)


class LocalPipeline:
    def __init__(self, spark: SparkSession, landing_path: str):
        self.spark = spark
        self.landing_path = landing_path
        self.result = RunResult()

    # ------------------------------------------------------------------ public
    def run(self, pipeline_dir: Path = PIPELINE_DIR) -> RunResult:
        for path in sorted(pipeline_dir.glob("*.sql")):
            for stmt in _split_statements(path.read_text()):
                self._execute(stmt, path.name)
        return self.result

    # ------------------------------------------------------------------ dispatch
    def _execute(self, stmt: str, source: str) -> None:
        if re.match(r"^APPLY\s+CHANGES", stmt, re.IGNORECASE):
            return self._apply_changes(stmt)
        m = CREATE_RE.match(stmt)
        if not m:
            raise ValueError(f"{source}: unsupported statement:\n{stmt[:200]}")
        name, rest = m.group(2), m.group(3).strip()

        constraints = []
        if rest.startswith("("):
            inside, rest = _take_parenthesized(rest)
            constraints = [CONSTRAINT_RE.match(c) for c in _split_top_level_commas(inside)]
            if not all(constraints):
                raise ValueError(f"{name}: could not parse constraints")
        as_match = re.search(r"\bAS\b\s*(WITH\b|SELECT\b|\()", rest, re.IGNORECASE)
        if not as_match:
            return  # a bare declaration, e.g. the target of APPLY CHANGES
        query = _strip_stream(rest[as_match.start(1) :])

        if "read_files(" in query:
            df = self._read_landing()
        else:
            df = self.spark.sql(query)
        df = self._apply_expectations(name, df, constraints)
        self._register(name, df)

    # ------------------------------------------------------------------ pieces
    def _read_landing(self) -> DataFrame:
        return (
            self.spark.read.option("recursiveFileLookup", "true")
            .text(self.landing_path)
            .select(
                F.col("value").alias("raw"),
                F.input_file_name().alias("source_file"),
                F.regexp_extract(F.input_file_name(), r"export_(\d{4}-\d{2}-\d{2})", 1).alias("export_date"),
                F.current_timestamp().alias("ingested_at"),
            )
            .where(F.length("raw") > 0)
        )

    def _apply_expectations(self, table: str, df: DataFrame, constraints) -> DataFrame:
        if not constraints:
            return df
        checks = [(c.group(1), c.group(2), (c.group(4) or "").upper()) for c in constraints]
        flagged = df.select(
            "*", *[F.coalesce(F.expr(expr), F.lit(False)).alias(f"__ok_{n}") for n, expr, _ in checks]
        ).localCheckpoint()
        counts = flagged.agg(*[F.sum((~F.col(f"__ok_{n}")).cast("int")).alias(n) for n, _, _ in checks]).first()
        for n, _, action in checks:
            failed = counts[n] or 0
            self.result.expectation_failures[(table, n)] = failed
            if action.startswith("FAIL") and failed:
                raise AssertionError(f"{table}: expectation {n} failed on {failed} rows (FAIL UPDATE)")
        keep = F.lit(True)
        for n, _, action in checks:
            if action.startswith("DROP"):
                keep = keep & F.col(f"__ok_{n}")
        return flagged.where(keep).drop(*[f"__ok_{n}" for n, _, _ in checks])

    def _apply_changes(self, stmt: str) -> None:
        m = APPLY_RE.match(stmt)
        if not m:
            raise ValueError(f"could not parse APPLY CHANGES:\n{stmt}")
        target, source, keys, delete_when, sequence, except_cols, scd_type, untracked = m.groups()
        keys = [k.strip() for k in keys.split(",")]
        except_cols = [c.strip() for c in (except_cols or "").split(",") if c.strip()]
        untracked = [c.strip() for c in (untracked or "").split(",") if c.strip()]
        src = self.spark.table(source).withColumn("__seq", F.expr(sequence.strip()))
        if delete_when:
            src = src.withColumn("__is_delete", F.coalesce(F.expr(delete_when.strip()), F.lit(False)))
        else:
            src = src.withColumn("__is_delete", F.lit(False))
        out_cols = [c for c in self.spark.table(source).columns if c not in except_cols]
        src.createOrReplaceTempView("__cdc_src")
        part = ", ".join(keys)

        if scd_type == "1":
            # latest event per key wins; if that event is a delete, the key is gone
            df = self.spark.sql(
                f"""
                SELECT * FROM (
                    SELECT *, row_number() OVER (PARTITION BY {part} ORDER BY __seq DESC) AS __rn
                    FROM __cdc_src
                ) WHERE __rn = 1 AND NOT __is_delete
                """
            ).select(*out_cols)
        else:
            tracked = [c for c in out_cols if c not in keys and c not in untracked]
            hash_expr = "sha2(to_json(named_struct(" + ", ".join(f"'{c}', {c}" for c in tracked) + ")), 256)"
            df = self.spark.sql(
                f"""
                WITH ordered AS (
                    SELECT *, {hash_expr} AS __hash,
                           lag({hash_expr}) OVER (PARTITION BY {part} ORDER BY __seq) AS __prev_hash
                    FROM __cdc_src WHERE NOT __is_delete
                ),
                versions AS (
                    SELECT * FROM ordered WHERE __prev_hash IS NULL OR __prev_hash <> __hash
                )
                SELECT *, __seq AS __START_AT,
                       lead(__seq) OVER (PARTITION BY {part} ORDER BY __seq) AS __END_AT
                FROM versions
                """
            ).select(*out_cols, "__START_AT", "__END_AT")
        self._register(target, df)

    def _register(self, name: str, df: DataFrame) -> None:
        df.localCheckpoint(eager=True).createOrReplaceTempView(name)
        self.result.tables.append(name)
