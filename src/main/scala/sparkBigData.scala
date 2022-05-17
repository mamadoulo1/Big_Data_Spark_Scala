import org.apache.spark.sql._
object SparkBigData {
  val ss : SparkSession = SparkSession.builder().appName("Mon application Spark")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()

}
