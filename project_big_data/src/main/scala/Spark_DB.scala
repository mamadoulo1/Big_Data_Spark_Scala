import sparkBigDataScala._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util._

object Spark_DB {
  def main(args: Array[String]): Unit = {
    val ss = SessionSpark(true)
    val props_mysql = new Properties
    props_mysql.put("user","consultant")
    props_mysql.put("password", "pwd#86")
    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jea_db","jea_db.orders", props_mysql)
    df_mysql.show(15)
    df_mysql.printSchema()


    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/jea_db" )
      .option("user", "consultant")
      .option("password", "pwd#86")
      .option("dbtable", "(select state, city , sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) table_summary")
      .load()

    df_mysql2.show()

    val props_postgres = new Properties
    props_postgres.put("user","postgres")
    props_postgres.put("password", "Abdoukha1@2")
    val df_postgres = ss.read.jdbc("jdbc:postgressql://127.0.0.1:5432/jea_db","jea_db.orders", props_postgres)
    df_postgres.show(15)



  }

}
