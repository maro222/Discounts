package Refactored_code

import scala.io.{Codec, Source}
import scala.util.{Try, Using}
import java.time.Instant
import java.sql.{Connection, DriverManager}
import java.nio.file.{Files, Paths, StandardOpenOption}

object Infrastructure {

  // ── Logging ─────────────────────────────────────────────────────────────────

  def log(level: String, message: String): Unit = {
    val logLine = s"${Instant.now()} $level $message\n"
    Files.write(
      Paths.get("rules_engine.log"),
      logLine.getBytes,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
  }

  // ── Database ──────────────────────────────────────────────────────────────────

  def writeToDb(records: List[ProcessedTransaction], url: String): Try[Unit] =
    Using(DriverManager.getConnection(url)) { connection =>


      // ── Fix 1: Disable auto-commit — wrap everything in ONE transaction ──────
      // Default: every INSERT is its own transaction = 10M disk flushes
      // With autoCommit=false: all INSERTs are buffered, ONE disk flush at end
      connection.setAutoCommit(false)

      connection.createStatement().execute(
        """CREATE TABLE IF NOT EXISTS orders (
            timestamp   TEXT,
            product     TEXT,
            expiry_date TEXT,
            quantity    INTEGER,
            unit_price  REAL,
            channel     TEXT,
            payment     TEXT,
            final_price REAL
          )"""
      )

      val stmt = connection.prepareStatement(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      )

      // addBatch() queues the INSERT in memory
      // executeBatch() sends the whole chunk to SQLite at once
      Using(connection.prepareStatement(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      )) { stmt =>

        records.grouped(10_000).foreach { batch =>
          batch.foreach { case ProcessedTransaction(t, finalPrice) =>
            stmt.setString(1, t.timestamp)
            stmt.setString(2, t.product)
            stmt.setString(3, t.expiryDate)
            stmt.setInt(4, t.quantity)
            stmt.setDouble(5, t.unitPrice)
            stmt.setString(6, t.channel)
            stmt.setString(7, t.payment)
            stmt.setDouble(8, finalPrice)
            stmt.addBatch()
          }
          stmt.executeBatch()
          stmt.clearBatch()
        }
        connection.commit()
      }
    } // Using() calls connection.close() automatically here — even on failure


  def verifyDb(url: String): Unit = {
    val conn   = DriverManager.getConnection(url)
    val result = conn.createStatement().executeQuery("SELECT * FROM orders LIMIT 20")
    while (result.next())
      println(s"${result.getString(1)}, ${result.getString(2)}, ${result.getDouble(8)}")
    conn.close()
  }
}
