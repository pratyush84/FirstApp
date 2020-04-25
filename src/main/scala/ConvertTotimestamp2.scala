
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ConvertTotimestamp2 {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("2018-05-03 14:56:00:928", "America/Chicago"),
      ("2018-05-03 14:56:00:928", "America/Chicago"),
      ("2018-05-03 14:56:00:928", "America/Chicago")
    ).toDF("origin_timestamp", "origin_timezone")

//Comment-1

    val df1 = df.withColumn("time_utc",
      to_utc_timestamp(to_timestamp(df.col("origin_timestamp"), "yyyy-MM-dd HH:mm:ss:SSS") , "America/Chicago"))
      .withColumn("milliseconds", substring(df.col("origin_timestamp"),-3,3))

/*    val col_list = ["time_utc","milliseconds"]
    df1.withColumn(concat( df1.col("time_utc"),df1.col("milliseconds")))*/

  }
}
