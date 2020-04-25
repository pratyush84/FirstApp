import org.apache.spark.sql.SparkSession

object Question1 {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-DropDuplicates")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      ("Rajesh",21,"London"),
      ("Suresh",28,"California"),
      ("Sam","26","Delhi"),
      ("Rajesh", 21, "Gurgaon"),
      ("Manish", 29, "banglore")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)

    // convert RDD of tuples to DataFrame by supplying column names
    val customerDF = customerRows.toDF( "name", "age" ,"city")

    println("*** Here's the whole DataFrame with duplicates")

    customerDF.printSchema()

    customerDF.show()

    // drop fully identical rows
    val withoutDuplicates = customerDF.dropDuplicates()

    println("*** Now without duplicates")

    withoutDuplicates.show()

    // drop fully identical rows
    val withoutPartials = customerDF.dropDuplicates(Seq("name", "state"))

    println("*** Now without partial duplicates too")

    withoutPartials.show()

  }
}