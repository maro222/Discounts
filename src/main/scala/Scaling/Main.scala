package Refactored_code

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneId}
import scala.util.{Failure, Success, Try, Using}
import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters.IterableIsParallelizable

//--------------This is how we replace Array[String]
//--------------We represent the order on case class and also the final proceesed Transaction in case class
//--------------This way we can access them safely and Immutable by default
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


object DiscountEngine {

  // Defining the rules
  val rules: List[(Transaction => Boolean, Transaction => Double)] = List(
    (qualifyExpiryDate, calculateExpiryDate),
    (qualifyCheeseAndWine, calculateCheeseAndWine),
    (qualifySoldOnMarch, calculateSoldOnMarch),
    (qualifyQuantity, calculateQuantity),
    (qualifyChannel, calculateChannel),
    (qualifyPayment_method, calculatePayment_method)
  )

  // parsing the orders (Array[List]) mutable state to Transactions (Transaction) Immutable State
  def parseTransaction(row: Array[String]): Option[Transaction] =
    Try(Transaction(
      timestamp = row(0),
      product = row(1),
      expiryDate = row(2),
      quantity = row(3).toInt,
      unitPrice = row(4).toDouble,
      channel = row(5),
      payment = row(6)
    )).toOption // returns None if parsing fails

  def avg(x: List[Double]): Double =
    if (x.isEmpty) 0.0 else x.sum / x.length

  def calculateDiscount(t: Transaction): Double = {
    val applicableDiscounts = rules.flatMap {
      case (qualify, calculate) =>
        if (qualify(t)) Some(calculate(t)) else None
    }
    avg(applicableDiscounts.sortBy(-_).take(2))
  }

  def process(t: Transaction): ProcessedTransaction = {
    val originalPrice = t.quantity * t.unitPrice
    val discount = calculateDiscount(t)
    val finalPrice = originalPrice - (discount * originalPrice)
    ProcessedTransaction(t, finalPrice)
  }


  // Implementing Rules
  def qualifyExpiryDate(t: Transaction): Boolean = {
    val order_date = Instant.parse(t.timestamp).atZone(ZoneId.of("UTC")).toLocalDate

    // Parse the string of expiry date
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expiry_date: LocalDate = LocalDate.parse(t.expiryDate, formatter)
    val daysBetween = ChronoUnit.DAYS.between(order_date, expiry_date)

    daysBetween < 30
  }

  def calculateExpiryDate(t: Transaction): Double = {
    val order_date = Instant.parse(t.timestamp).atZone(ZoneId.of("UTC")).toLocalDate
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expiry_date: LocalDate = LocalDate.parse(t.expiryDate, formatter)

    val days = ChronoUnit.DAYS.between(order_date, expiry_date)
    (30 - days).toDouble / 100
  }

  def qualifyCheeseAndWine(t: Transaction): Boolean = {
    val words = t.product.split(" ")
    words.contains("Wine") || words.contains("Cheese")
  }

  def calculateCheeseAndWine(t: Transaction): Double = {
    if (t.product.split(" ").contains("Wine")) 0.05
    else 0.10
  }

  def qualifySoldOnMarch(t: Transaction): Boolean =
    t.timestamp.slice(5, 7) == "03" && t.timestamp.slice(8, 10) == "23"

  def calculateSoldOnMarch(t: Transaction): Double = {
    0.5
  }

  def qualifyQuantity(t: Transaction): Boolean = {
    t.quantity > 5
  }

  def calculateQuantity(t: Transaction): Double = {
    if ((t.quantity > 5) && (t.quantity < 10)) (5.0 / 100)
    else if ((t.quantity > 9) && (t.quantity < 15)) (7.0 / 100)
    else (10.0 / 100)
  }

  def qualifyChannel(t: Transaction): Boolean = {
    (t.channel == "App")
  }

  def calculateChannel(t: Transaction): Double = {
    (((t.quantity % 5) + 1) * 5.0) / 100.0
  }

  def qualifyPayment_method(t: Transaction): Boolean = {
    (t.payment == "Visa")
  }

  def calculatePayment_method(t: Transaction): Double = {
    0.05
  }
}


object Main extends App {

  val dbUrl = "jdbc:sqlite:H:/omar/ITI - Data Management/Scala/Discounts/orders.db"
  val csvPath = "H:/omar/ITI - Data Management/Scala/Discounts/src/main/scala/TRX10M.csv"


  // ── Thread pool — capped at number of CPU cores ───────────────────────────
  val numThreads = Runtime.getRuntime.availableProcessors()
  val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(numThreads))
  Infrastructure.log("INFO", s"Starting — $numThreads threads available")

  // ── Helper: run a transformation in parallel with the capped pool ─────────
  def parWithCap[A, B](batch: Seq[A])(f: A => B): Seq[B] = {
    val par = batch.par
    par.tasksupport = taskSupport
    par.map(f).toList
  }

  val totalStart = System.nanoTime()

  // ── Step 1: Open CSV as a lazy iterator — nothing loaded into RAM yet ──────
  val source = scala.io.Source.fromFile(csvPath)

  val processingStart = System.nanoTime()

  val processed: List[ProcessedTransaction] =
    try {
      source
        .getLines()
        .drop(1)           // skip header lazily
        .grouped(10_000)   // read 10k lines at a time
        .zipWithIndex
        .flatMap { case (batch, idx) =>

          // ── Step 2: Parse lines → Transaction objects (parallel) ───────────
          val transactions: Seq[Transaction] =
            parWithCap(batch) { line =>
              DiscountEngine.parseTransaction(line.split(","))
            }.flatten      // discard None (malformed rows)

          // ── Step 3: Apply discount rules → ProcessedTransaction (parallel) ──
          val results: Seq[ProcessedTransaction] =
            parWithCap(transactions)(DiscountEngine.process)

          Infrastructure.log("INFO", s"Batch ${idx + 1}: ${results.length} rows processed")

          results          // batch goes out of scope here → GC reclaims it
        }
        .toList
    } finally {
      source.close()       // always runs, even if an exception is thrown
    }

  val processingEnd = System.nanoTime()
  Infrastructure.log("INFO", s"Processing took ${(processingEnd - processingStart) / 1_000_000} ms")
  Infrastructure.log("INFO", s"All batches done — ${processed.length} total rows")

  // Writting to database
  val dbStart = System.nanoTime()
  Infrastructure.writeToDb(processed, dbUrl) match {
    case Success(_) =>
      Infrastructure.log("INFO", s"Written ${processed.length} rows to DB")
    case Failure(e) =>
      Infrastructure.log("ERROR", s"DB write failed: ${e.getMessage}")
  }

  val dbEnd = System.nanoTime()
  Infrastructure.log("INFO", s"DB write took ${(dbEnd - dbStart) / 1_000_000} ms")
  val totalEnd = System.nanoTime()
  Infrastructure.log("INFO", s"Total execution took ${(totalEnd - totalStart) / 1_000_000} ms")

  // verify by reading from table first 20 rows
  Infrastructure.verifyDb(dbUrl)
}