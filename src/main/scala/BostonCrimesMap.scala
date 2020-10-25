import java.nio.file.Paths

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, broadcast, collect_list, count, date_format, row_number, split, udf, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.expressions.Window
//import org.apache.log4j.{Level, Logger}


object BostonCrimesMap extends App {

  override def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.OFF)

    if (args.length != 3) {
      throw new Exception(
        "You must pass 3 arguments: {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}"
      )
    }

    val (crimeCsvPath, offenceCodesCsvPath, outputFolderPath) = (args(0), args(1), args(2))
    val timestamp: Long = System.currentTimeMillis / 1000
    val outputFilePath = Paths.get(outputFolderPath, f"output$timestamp.parquet").toString

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("loading crime.csv...")
    val crimeFacts = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeCsvPath)
      .withColumn(
        "year_month",
        date_format(
          unix_timestamp($"OCCURRED_ON_DATE", "yyyy-MM-dd HH:mm:ss").cast(TimestampType),
          "yyyyMM"))

    println("loading offense_codes.csv...")
    val offenseCodes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenceCodesCsvPath)
      .withColumn("crime_type", split($"NAME", " - ")(0))

    val offenseCodesBroadcast = broadcast(offenseCodes)

    println("aggregating...")
    val crimeFactsExtended = crimeFacts
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
      .select("INCIDENT_NUMBER", "DISTRICT", "year_month", "Lat", "Long", "crime_type")

    val resultDf01 = crimeFactsExtended
      .groupBy($"DISTRICT")
      .agg(
        count($"INCIDENT_NUMBER").alias("crimes_total"),
        avg($"Lat").alias("lat"),
        avg($"Long").alias("lng")
      )

    crimeFactsExtended
      .groupBy($"DISTRICT", $"year_month")
      .agg(count($"INCIDENT_NUMBER").alias("number_of_crimes"))
      .createOrReplaceTempView("crimeFactsExtended")

    val resultDf02 = spark.sql(
      """
        SELECT
          DISTRICT,
          percentile_approx(number_of_crimes, 0.5) AS crimes_monthly
        FROM crimeFactsExtended GROUP BY DISTRICT
      """.stripMargin)

    def frequentCrimeType = udf(
      (l: mutable.WrappedArray[String]) => l.toList.mkString(", ")
    )

    val resultDf03 = crimeFactsExtended
      .groupBy($"DISTRICT", $"crime_type")
      .agg(count($"INCIDENT_NUMBER").alias("number_of_crimes"))
      .withColumn("rn", row_number.over(
        Window.partitionBy($"DISTRICT").orderBy($"number_of_crimes".desc)
      ))
      .where($"rn" <= 3)
      .drop($"rn")
      .groupBy($"DISTRICT")
      .agg(collect_list($"crime_type").alias("crime_type_list"))
      .withColumn("frequent_crime_types", frequentCrimeType($"crime_type_list"))
      .drop($"crime_type_list")

    val resultDfFinal = resultDf01
      .join(resultDf02, Seq("DISTRICT"))
      .join(resultDf03, Seq("DISTRICT"))
      .orderBy($"crimes_total".desc)

//    resultDfFinal.show(false)
    resultDfFinal.write.parquet(outputFilePath)

  }
}
