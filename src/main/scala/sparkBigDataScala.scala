import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
object sparkBigDataScala {

  def main(args: Array[String]): Unit = {
    val sc = SessionSpark(Env = true).sparkContext
    val rdd_test : RDD[String]=sc.parallelize(List("alain", "juvenal", "anna"))
    rdd_test.foreach{
      l => println(l)
    }
  }
  var ss: SparkSession= null
  /**
   *fonction qui initialise et instancie une session Spark
   * @param Env C'est une variable qui indique l'environnement sur lequel notre application est déployée
   * Si Env =True , l'application est déployée en local et si False l'application est déployée dans un cluster.
   */


  def SessionSpark (Env: Boolean = true) : SparkSession = {
    if (Env==true){
      System.setProperty("hadoop.home.dir","C:/hadoop/bin")
      ss = SparkSession.builder
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          .enableHiveSupport()
          .getOrCreate()
    } else {
      ss = SparkSession.builder().appName("Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()

    }
   ss

  }


}
