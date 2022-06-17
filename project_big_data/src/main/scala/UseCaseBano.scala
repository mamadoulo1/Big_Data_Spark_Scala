import sparkBigDataScala._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
object UseCaseBano {

  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", IntegerType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)


  ))

  val configh = new Configuration()
  val fs = FileSystem.get(configh)
  val chemin_dest1 = new Path("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\destinantion\\1")
  val chemin_dest2 = new Path("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\destinantion\\2")
  def main(args: Array[String]): Unit = {
    val sc = SessionSpark(Env = true).sparkContext
    val session_s = SessionSpark(Env = true)
    sc.setLogLevel("OFF")



    //Load dataset
    val ss = SessionSpark(true)

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_bano)
      .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\UseCaseBano\\full.csv")
     df_bano_brut.show(10)

    val df_bano = df_bano_brut
      .withColumn("code_department", substring(col("code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreeMap"))
          .otherwise(when(col("code_source_bano") === lit("DD"), lit("OpenData"))
            .otherwise(when(col("code_source_bano") === lit("O+O"), lit("OpenData OSM"))
              .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
                .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM"))
              )))))

    df_bano.show(10)

    val df_departement = df_bano.select(col("code_department")).distinct().filter(col("code_department").isNotNull)
    val liste_departmement = df_bano.select(col("code_department")).distinct().filter(col("code_department").isNotNull).collect()
      .map(x => x(0)).toList
    //liste_departmement.foreach(e => println(e.toString))
    df_departement.show()


    liste_departmement.foreach{
      x => df_bano.filter(col("code_department")===x.toString)
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\UseCaseBano\\Bano_complet\\bano" + x.toString)
        val chemin_sce = new Path("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\UseCaseBano\\Bano_complet\\bano"+x.toString )

      fs.copyFromLocalFile(chemin_sce, chemin_dest1)
    }


/*
    df_departement.foreach{
      dep => df_bano.filter(col("code_department")=== dep.toString)
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\UseCaseBano\\Bano_complet\\bano" + dep.toString)
        val chemin_scr = new Path("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\UseCaseBano\\Bano_complet\\bano"+dep.toString )
      fs.copyFromLocalFile(chemin_scr, chemin_dest2)
    }

     */



  }
}
