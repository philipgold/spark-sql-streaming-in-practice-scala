package il.nayatech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.util.parsing.json._
import org.apache.spark.sql.functions._

/**
  * About csv file Flight-History. The CSV files have six (6) columns containing time, position, and movement information.
  *
  * 1) Timestamp - listed Unix Epoch time.
  * 2) UTC
  * 3) Callsign - Airline ID (contains the three-character ICAO airline identifier)
  * 4) Position - contains data, reported as latitude and longitude.
  * 5) Altitude (in feet)
  * 6) Speed (Ground Speed in Knots)
  * 7) Direction
  */

object FlightHistory{

  def main(args: Array[String]): Unit = {

    val bootstrapServers = "localhost:9092"
    val subscribeType = "subscribe"
    val topics = "Cads"

    val conf = new SparkConf().setMaster("local[2]")

    val spark = SparkSession
      .builder
      .appName("FlightHistory")
      .config(conf)
      .getOrCreate()

    // Create dataframe representing the stream of input json from kafka
    val dfKafkaRaw = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp as STRING)")

    dfKafkaRaw.printSchema()

    // This import is needed to use the $-notation
    import spark.implicits._


    // Convert to JSON and then parse json value
    val jsonSchema = new StructType()
      .add("Timestamp", TimestampType)
      .add("UTC", StringType)
      .add("Callsign", StringType)
      .add("Position", StringType)
      .add("Altitude", IntegerType)
      .add("Speed", IntegerType)
      .add("Direction", IntegerType)


    var dfJson = dfKafkaRaw.select(
      from_json($"value",jsonSchema).alias("parsed_json_values"),
      $"timestamp".alias("KafkaTimestamp").cast(TimestampType)
    )

    dfJson = dfJson.select(
      $"parsed_json_values.*",
      $"KafkaTimestamp"
    )

    dfJson.printSchema()

    // Add UDF column - Converting Knots to kilometer
    val upperUDF = udf((x: Int) => x * 1.852) // Knots (kts) to kilometers/hour (kph)'''
    val dfMain = dfJson.withColumn("speed_kph", upperUDF($"speed"))

    dfMain.printSchema()


    /**
      * Create addition Dataframe for Feature Extractions (dfFE)
      * 1) Callsign -
      * 2) atd_timestamp - ATD stands for ‘Actual Time of Departure’
      * 3) ata_timestamp - ATA stands for ‘Actual Time of Arrival’
      * 4) flown_hours - Total flight hour from the benning till last event
      */

    // Adding atd_timestamp, ata_timestamp columns to Dataframe for Feature Extractions (dfFE)
    var dfFE = dfMain.groupBy(
      $"Callsign"
    ).agg(
      min("Timestamp").alias("atd_timestamp"),
      max("Timestamp").alias("ata_timestamp"),
      first("Position").alias("begin_position"),
      last("Position").alias("end_position")
    )

    dfFE.printSchema()

    // Adding total_flown_hours to Dataframe for Feature Extractions (dfFE)
    val diff_secs_col = col("ata_timestamp").cast("long") - col("atd_timestamp").cast("long")
    dfFE = dfFE.withColumn("total_flown_hours", diff_secs_col / 3600D)

    dfFE.printSchema()

    // Calculate how many kilometers each plane flew from beginning to end for Feature Extractions (dfFE)
    val flownDistanceUDF = udf((begin:String, end:String) => getDistance(begin, end))

    dfFE = dfFE.withColumn("total_flown_distance",
      flownDistanceUDF(col("begin_position"), col("end_position")))

    dfFE.printSchema()

    // Register a streaming dfFE DataFrame as a temporary view
    dfFE.createOrReplaceTempView("flight_fe_df")

    /**
      *  2 Case - Добавляем колонку, где вычисление в рамках одного окна, т.е. Используем window functions. Вычислить
      *  среднию скорость на последний час
     */

    // Group the data by window and Callsign and compute the average of each group
    val dfMainGrouped = dfMain.groupBy(
      window(col("UTC"), "1 hour"),
      col("Callsign")
    ).avg("speed_kph").orderBy("window")

    dfMainGrouped.printSchema()


    // Start running the query that prints the running computed data to the console
    val query = dfMainGrouped.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }

  /**
    * Calculate the great circle distance between two points
    * on the earth (specified in decimal degrees). Using Haversine formula.
    */
  def getDistance(beginPosition: String, endPosition: String): Double ={
    // convert string position to decimal degrees
    val lat1:Double = beginPosition.split(",").apply(0).toDouble
    val lon1:Double = beginPosition.split(",").apply(1).toDouble
    val lat2:Double = endPosition.split(",").apply(0).toDouble
    val lon2:Double = endPosition.split(",").apply(1).toDouble

    // convert decimal degrees to radians
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians
    import math._
    // Haversine formula
    val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2 * asin(sqrt(a))
    val R = 6372.8  //radius of the Earth in km
    R * c
  }
}