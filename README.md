# Scala Discount Rules Engine

A high-performance transaction processing engine written in Scala that reads millions of orders from a CSV file, applies configurable discount rules, and persists results to a SQLite database.

---

## Overview

This project was built and then scaled through two distinct versions:

- **V1 — Refactored:** Clean, functional, type-safe implementation for correctness
- **V2 — Scaled:** Streaming + parallel processing to handle 10M+ rows without running out of memory

---

## Project Structure

```
src/main/scala/
├── Transaction.scala          # Domain model — immutable case classes
├── DiscountEngine.scala       # Pure discount rule functions
├── Infrastructure.scala       # Side effects — file I/O, logging, database
└── Main.scala                 # Entry point — orchestrates the pipeline
```

---

## Domain Model

All data is represented as immutable case classes, replacing raw `Array[String]` with typed, safe structures:

```scala
case class Transaction(
  timestamp:  String,
  product:    String,
  expiryDate: String,
  quantity:   Int,
  unitPrice:  Double,
  channel:    String,
  payment:    String
)

case class ProcessedTransaction(
  transaction: Transaction,
  finalPrice:  Double
)
```

---

## Discount Rules

Six rules are evaluated per transaction. The top two discounts by value are averaged and applied:

| Rule | Condition | Discount |
|---|---|---|
| Expiry date | Product expires within 30 days | `(30 - daysLeft) / 100` |
| Cheese & Wine | Product contains "Cheese" or "Wine" | 10% / 5% |
| Sold on March 23 | Transaction timestamp is March 23 | 50% |
| Quantity | Quantity > 5 | 5% / 7% / 10% based on amount |
| App channel | Purchase made via App | `((qty % 5) + 1) × 5%` |
| Visa payment | Payment method is Visa | 5% |

Rules are defined as a list of `(qualify, calculate)` function pairs, making it easy to add or remove rules without changing the engine logic.

---

## V1 — Refactored (Correctness-first)

### Pipeline

```
CSV file → readLines (List[String])
         → parse    (List[Transaction])
         → process  (List[ProcessedTransaction])
         → writeToDb
         → verifyDb
```

### Key design decisions

**Immutable data** — case classes replace mutable `Array[String]`. Fields are accessed by name, not index.

**Safe parsing** — `parseTransaction` wraps construction in `Try(...).toOption`, silently dropping malformed rows instead of crashing.

**Separation of concerns** — pure functions (`DiscountEngine`) are isolated from side effects (`Infrastructure`). The engine has no knowledge of files or databases.

**Error handling** — `Try[T]` is returned from all I/O operations and matched at the call site, never thrown silently.

### Limitation at scale

V1 loads the entire CSV into memory as three full `List` objects simultaneously:

```
rawLines      → List[String]               ~1 GB for 10M rows
transactions  → List[Transaction]          ~2 GB
processed     → List[ProcessedTransaction] ~3 GB
─────────────────────────────────────────────────
Peak heap                                  ~6 GB  ← OutOfMemoryError
```

---

## V2 — Scaled (Memory-efficient)

### What changed and why

| Problem in V1 | Fix in V2 |
|---|---|
| Full file loaded into RAM | Lazy `Iterator` via `Source.fromFile` |
| 3 full lists alive simultaneously | Single fused pipeline — process and write per batch |
| Unbounded ForkJoin task explosion | Capped `ForkJoinPool(nCores)` |
| 10M individual INSERT statements | `addBatch` + `executeBatch` per 10k rows |
| `prepareStatement` called 10M times | Prepared once, reused |
| Connection leak on failure | `Using(connection)` — auto-closed |

### Pipeline

```
Source.fromFile (lazy, 0 bytes read)
  → .getLines().drop(1)
  → .grouped(10_000)          ← 10k lines at a time
  → .zipWithIndex
  → .foreach { batch =>
       parWithCap → parse     ← 8 threads
       parWithCap → process   ← 8 threads
       stmt.addBatch / executeBatch  ← write immediately
       batch out of scope → GC
     }
  → connection.commit()       ← single atomic flush
```

### Memory profile

```
V1:  [==1GB==] → [======3GB======] → [=========6GB=========]  CRASH

V2:  [~3MB][~3MB][~3MB][~3MB] ... [~3MB]   flat across all 10M rows
```

### Parallelism

A helper function `parWithCap` handles both the parse and process steps:

```scala
def parWithCap[A, B](batch: Seq[A])(f: A => B): Seq[B] = {
  val par = batch.par
  par.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(nCores))
  par.map(f).toList
}
```

- `[A, B]` — generic: works for both `String → Transaction` and `Transaction → ProcessedTransaction`
- Two parameter lists — allows clean `{ block }` syntax for the function argument
- `tasksupport` — caps threads to CPU core count, preventing ForkJoin recursive task explosion

### Database write optimisation

```scala
connection.setAutoCommit(false)          // buffer all writes

records.grouped(10_000).foreach { batch =>
  batch.foreach { stmt.addBatch() }      // queue in memory
  stmt.executeBatch()                    // flush 10k at once
  stmt.clearBatch()
}

connection.commit()                      // one disk flush for all 10M rows
```

Without this, 10M individual `INSERT` + `commit` cycles would take hours on SQLite.

---

## Running the Project

### Requirements

- Scala 3.x
- sbt
- SQLite JDBC driver on classpath

### JVM options (required for large files)

Add to `build.sbt`:

```scala
javaOptions ++= Seq("-Xms512m", "-Xmx4g")
fork := true
```

### Run

```bash
sbt run
```

### Output

- `rules_engine.log` — timestamped log of every batch and timing
- `orders.db` — SQLite database with the `orders` table
- Console — first 20 rows printed by `verifyDb`

---

## Logging

All log entries are appended to `rules_engine.log`:

```
2024-01-15T10:23:01Z [INFO] Starting — 8 threads available
2024-01-15T10:23:04Z [INFO] Batch 1: 9998 rows written
2024-01-15T10:23:07Z [INFO] Batch 2: 10000 rows written
...
2024-01-15T10:31:22Z [INFO] Done — 10000000 rows total
2024-01-15T10:31:22Z [INFO] DB write took 48302 ms
2024-01-15T10:31:22Z [INFO] Total execution took 93847 ms
```

---

## Key Scala Concepts Used

| Concept | Where | Purpose |
|---|---|---|
| Case classes | `Transaction`, `ProcessedTransaction` | Immutable, pattern-matchable domain models |
| `Option[T]` | `parseTransaction` | Safe handling of malformed rows |
| `Try[T]` | All I/O operations | Explicit error handling without exceptions |
| Higher-order functions | `rules` list, `parWithCap` | Functions passed as values |
| Generic types `[A, B]` | `parWithCap` | One function works for any input/output types |
| Curried parameter lists | `parWithCap(batch)(f)` | Clean block syntax, left-to-right type inference |
| Lazy `Iterator` | `Source.fromFile` | Stream without loading into memory |
| Parallel collections | `.par` + `tasksupport` | CPU-level parallelism with controlled thread count |
| `Using` | DB connection | Guaranteed resource cleanup |
| Pattern matching | `flatMap { case (qualify, calculate) }` | Destructure tuples cleanly |
