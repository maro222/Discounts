import scala.io.{Codec, Source}
import java.time.{Instant, LocalDate, ZoneId}
import java.time.temporal.ChronoUnit
import java.time.format.DateTimeFormatter
import scala.util.{Try, Using, Failure, Success}
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.nio.file.{Files, Paths, StandardOpenOption}

object Main extends App{

  def readFile(fileName: String, codec: String = Codec.default.toString): Try[List[String]] = {
    Using(Source.fromFile(fileName, codec))(_.getLines().toList)  }

  val orders: List[String] =
    readFile("H:/omar/ITI - Data Management/Scala/Discounts/src/main/scala/TRX1000.csv") match {
      case Success(lines) =>
        log("INFO", s"Successfully read ${lines.length} transactions")
        lines
      case Failure(e) =>
        log("ERROR", s"Failed to read CSV: ${e.getMessage}")
        Nil   // fallback
    }

  val rules: List[(Array[String] => Boolean, Array[String] => Double)] = List(
    (qualify_expiry_date, calculate_expiry_date),
    (qualify_cheese_and_wine, calculate_cheese_and_wine),
    (qualify_sold_on_march, calculate_sold_on_march),
    (qualify_quantity, calculate_quantity)
  )

  val transactions = orders
    .tail
    .map(x => x.split(","))     //[(c1,c2,...), (c1,c2,...), (c1,c2,...), ....]


  val prices = transactions
    .map(x => x(3).toDouble * x(4).toDouble)


  val Newprices = transactions
    .map(calculate_discount)
    .zip(prices)
    .map(x => x._2 - ( x._1 * x._2))

  val transactionDB =
    transactions
      .zip(Newprices)
  //  transactionDB.foreach(println)


  //----------------Database Part------------------------//
  val url = "jdbc:sqlite:orders.db" // SQLite example
  val connection: Connection = DriverManager.getConnection(url)

  val createTable = connection.createStatement()
  createTable.execute(
    """CREATE TABLE IF NOT EXISTS orders (
      timestamp TEXT,
      product TEXT,
      expiry_date TEXT,
      quantity INTEGER,
      unit_price REAL,
      channel TEXT,
      payment TEXT,
      final_price REAL
    )"""
  )
  // Table created successfully
  log("INFO", "Database table created successfully")

  Try(transactionDB.foreach { case (tx, finalPrice) =>
    val stmt = connection.prepareStatement(
      "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    stmt.setString(1, tx(0))
    stmt.setString(2, tx(1))
    stmt.setString(3, tx(2))
    stmt.setInt(4, tx(3).toInt)
    stmt.setDouble(5, tx(4).toDouble)
    stmt.setString(6, tx(5))
    stmt.setString(7, tx(6))
    stmt.setDouble(8, finalPrice)
    stmt.executeUpdate()
  }) match {
    case Success(_) => log("INFO", s"Successfully written ${transactionDB.length} rows to database")
    case Failure(e) => log("ERROR", s"Database write failed: ${e.getMessage}")
  }
  // After all rows inserted
  log("INFO", s"Successfully written ${transactionDB.length} rows to database")
  connection.close()

  // ---- verify ----- //
  val checkConnection = DriverManager.getConnection(url)
  val result = checkConnection.createStatement().executeQuery("SELECT * FROM orders LIMIT 5")
  while (result.next()) {
    println(s"${result.getString(1)}, ${result.getString(2)}, ${result.getDouble(8)}")
  }
  checkConnection.close()




  //----------Logging-------------//
  def log(level: String, message: String): Unit = {
    val timestamp = Instant.now().toString
    val logLine = s"$timestamp $level $message\n"
    Files.write(
      Paths.get("rules_engine.log"),
      logLine.getBytes,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
  }










  //---------Helper Methods-----------//
  def calculate_discount(transaction: Array[String]): Double = {
    val discounts =
      rules.flatMap { case (qualify, calculate) =>
        if (qualify(transaction)) Some(calculate(transaction)) else None
      }

    val average = avg(discounts
      .sortBy(-_)
      .take(2))

    average
  }

  def avg(discounts: List[Double]): Double = {
    if (discounts.isEmpty) 0.0
    else discounts.sum / discounts.length.toDouble
  }



  //-------Rules-----------//
  def qualify_expiry_date(transaction: Array[String]): Boolean = {
    val order_date = Instant.parse(transaction(0)).atZone(ZoneId.of("UTC")).toLocalDate

    // Parse the string of expiry date
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expiry_date: LocalDate = LocalDate.parse(transaction(2), formatter)
    val daysBetween = ChronoUnit.DAYS.between(order_date, expiry_date)

    if (daysBetween < 30)
      true
    else
      false
  }

  //returning discount
  def calculate_expiry_date(transaction: Array[String]): Double = {
    val order_date = Instant.parse(transaction(0)).atZone(ZoneId.of("UTC")).toLocalDate

    // Parse the string of expiry date
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val expiry_date: LocalDate = LocalDate.parse(transaction(2), formatter)
    val daysBetween = ChronoUnit.DAYS.between(order_date, expiry_date)

    val discount =((30-daysBetween).toDouble / 100)
    discount
  }

  def qualify_cheese_and_wine(transaction: Array[String]): Boolean = {
    val name = transaction(1).split(" ")
    if (name.contains("Wine")) true
    else if (name.contains("Cheese")) true
    else false
  }

  def calculate_cheese_and_wine(transaction: Array[String]): Double = {
    val name = transaction(1).split(" ")
    if (name.contains("Wine")) {(5.0/100)}
    else {(10.0/100)}
  }

  def qualify_sold_on_march(transaction: Array[String]): Boolean = {
    val order_day = transaction(0).slice(8, 10)
    val order_month = transaction(0).slice(5, 7)

    if ((order_day == "23") && (order_month == "03")) true
    else false
  }

  def calculate_sold_on_march(transaction: Array[String]): Double = {
    0.5
  }

  def qualify_quantity(transaction: Array[String]): Boolean = {
    val quantity = transaction(3).toInt

    if (quantity > 5) true
    else false
  }

  def calculate_quantity(transaction: Array[String]): Double = {
    val quantity = transaction(3).toInt
    if ((quantity > 5) && (quantity < 10)) (5.0/100)
    else if ((quantity > 9) && (quantity < 15)) (7.0/100)
    else (10.0/100)
  }

}

