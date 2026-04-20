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

  // ── File I/O ─────────────────────────────────────────────────────────────────

  def readLines(path: String): Try[List[String]] =
    Using(Source.fromFile(path, Codec.default.toString))(_.getLines().toList)

  // ── Database ─────────────────────────────────────────────────────────────────

  def writeToDb(records: List[ProcessedTransaction], url: String): Try[Unit] = Try {
    val connection: Connection = DriverManager.getConnection(url)

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

    records.foreach { case ProcessedTransaction(t, finalPrice) =>
      val stmt = connection.prepareStatement(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      )
      stmt.setString(1, t.timestamp)
      stmt.setString(2, t.product)
      stmt.setString(3, t.expiryDate)
      stmt.setInt(4,    t.quantity)
      stmt.setDouble(5, t.unitPrice)
      stmt.setString(6, t.channel)
      stmt.setString(7, t.payment)
      stmt.setDouble(8, finalPrice)
      stmt.executeUpdate()
    }

    connection.close()
  }

  def verifyDb(url: String): Unit = {
    val conn   = DriverManager.getConnection(url)
    val result = conn.createStatement().executeQuery("SELECT * FROM orders LIMIT 5")
    while (result.next())
      println(s"${result.getString(1)}, ${result.getString(2)}, ${result.getDouble(8)}")
    conn.close()
  }
}
