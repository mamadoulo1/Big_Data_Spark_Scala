import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf



object sparkBigDataScala {
  val schema_order = StructType(
    Array(
      StructField("orderid", IntegerType, false),
      StructField("customerid", IntegerType, false),
      StructField("campaignid", IntegerType, true),
      StructField("orderdate", TimestampType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipcode", StringType, true),
      StructField("paymenttype", StringType, true),
      StructField("totalprice", DoubleType, true),
      StructField("numorderlines", IntegerType, true),
      StructField("numunits", IntegerType, true)

    )
  )
   def main(args: Array[String]): Unit = {

     val sc = SessionSpark(Env = true).sparkContext
     val session_s = SessionSpark(Env = true)
     sc.setLogLevel("OFF")

     val df_test = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", ",")
       .option("header", true)
       .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2010-12-06.csv")
     val df_goup = session_s.read
         .format("csv")
         .option("inferSchema", "true")
       .option("header", "true")
         .load("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\csvs\\")

     val df_goup2 = session_s.read
       .format("csv")
       .option("inferSchema", "true")
       .option("header", "true")
       .load("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2010-12-06.csv", "C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2011-01-20.csv")

     //df_goup2.show(5)
     //println("df_group count: "+ df_goup2.count())

     df_test.printSchema()
     //df_test.columns
     val df_2 = df_test.select(
       col("InvoiceNo"),
       col("_c0").alias("ID du client"),
       col("StockCode").cast(IntegerType).alias("Code de la marchandise"),
       col("Invoice".concat("No")).alias("ID  de la commande")
     )
     val df_3  = df_test.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType))
         .withColumn("StockCode", col("StockCode").cast(IntegerType))
         .withColumn("valeur_constante", lit(50))
         .withColumnRenamed("_c0", "ID_client")
         .withColumn("ID_commande", concat_ws("|", col("InvoiceNo"), col("ID_client")))
         .withColumn("total_amount", round(col("Quantity")*col("UnitPrice"), 2))
         .withColumn("Created_dt", current_timestamp())
         .withColumn("reduction_test", when(col("total_amount")>15, lit(3)).otherwise(lit(0)))
       .withColumn("reduction",when(col("total_amount")<15,lit(0))
                 .otherwise(when(col("total_amount").between(15, 20), lit(3))
                 .otherwise(when(col("total_amount")>15,lit(0)))))
         .withColumn("net_income", col("total_amount")-col("reduction"))

     val df_not_reduced = df_3.filter(col("reduction")=== lit(0) || col("Country").isin("France", "USA"))

     //df_not_reduced.show(10)

     //df jointure
     val df_ordres = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", "\t")
       .option("header", true)
       .schema(schema_order)
       .load("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\orders.txt")
     df_ordres.printSchema()
     df_ordres.show(5)
     //df_ordres.printSchema()
     val df_ordersGood = df_ordres.withColumnRenamed("numunits", "numunits_orders")
       .withColumnRenamed("totalprice", "totalprice_orders")

     val df_products = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", "\t")
       .option("header", true)
       .load("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\product.txt")


     val df_orderline = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", "\t")
       .option("header", true)
       .load("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\orderline.txt")
     //df_orderline.printSchema()
     val df_join_orders = df_orderline.join(df_ordersGood, df_ordres.col("orderid")=== df_orderline.col("orderid"), "inner")
         .join(df_products,df_products.col("productid")===df_orderline.col("productid"), Inner.sql)
     //df_join_orders.printSchema()


     val df_fichier1 = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", ",")
       .option("header", true)
       .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2010-12-06.csv")
     val df_fichier2 = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", ",")
       .option("header", true)
       .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2011-01-20.csv")

     val df_fichier3 = session_s.read
       .format("com.databricks.spark.csv")
       .option("delimiter", ",")
       .option("header", true)
       .csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\2011-12-08.csv")
     val df_united_files = df_fichier1.union(df_fichier2)
       .union(df_fichier3)
     //println(df_united_files.count())

     df_join_orders.withColumn("total_amount", round(col("numunits")* col("totalprice"), 2))
       .groupBy("state","city")
       .sum("total_amount").alias("commandes_totales")

     //Opérations de fénétrage
     val wn_spec = Window.partitionBy(col("state")).orderBy(col("state").desc)
     val df_join_windon = df_join_orders.withColumn("ventes_dep", sum(round(col("numunits")* col("totalprice"), 2)).over(wn_spec))
       .select(
         col("orderlineid"),
         col("zipcode"),
         col("PRODUCTGROUPNAME"),
         col("state"),
         col("ventes_dep")

       )
     df_join_orders.createOrReplaceTempView("orders")
     df_join_orders.withColumn("total_amount", round(col("numunits")* col("totalprice"), 2))
       .groupBy("state","city")
       .sum("total_amount").alias("commandes_totales")

     session_s.sql("select state, city , sum(round(numunits* totalprice), 2)) as commandes_totales from orders group by state, city").show()





     //df_join_windon.repartition(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Fichiers sources\\sources de données\\Ecriture")
def spark_hdfs(): Unit={
  val config_fs = SessionSpark(true).sparkContext.hadoopConfiguration
  val fs = FileSystem.get(config_fs)
  val scr_path = new Path("/user/datalake/marketing")
  val dest_path = new Path("/user/datalake/index")
  val ren_scr = new Path("/user/datalake/marketing/fichier_reporting.parquet")
  val dest_scr = new Path("/user/datalake/marketing/reporting.parquet")

  //Lecture des fichiers d'un dossier
  val files_file = fs.listStatus(scr_path).map(x => x.getPath)
  for(i <- 1 to files_file.length){
    println(files_file(i))
  }
  //renommer un fichier
  fs.rename(ren_scr, dest_scr)
  //supprimer les fichiers d'un dossier
  fs.delete(dest_scr, true)
  //copier un fichier du répertoire local vers le cluster
  //fs.copyFromLocalFile(dest_scr, ren_scr)


}
     df_ordersGood.withColumn("date_lecture", date_format(current_date(), "dd/MM/yyyy"))
       .withColumn("date_lecture_complete", current_timestamp())
       .withColumn("périodes_secondes", window(col("orderdate"), "10 minutes"))
       .select(
         col("orderdate"),
         col("périodes_secondes"),
         col("périodes_secondes.start"),
         col("périodes_secondes.end")
       )
       .show(5)
     df_united_files.show(5)
     df_united_files.printSchema()
     df_united_files.withColumn("InvoiceDate", to_date(col("InvoiceDate")))
       .withColumn("InvoiceTimeStamp", to_timestamp(col("InvoiceTimeStamp").cast(TimestampType)))
       .withColumn("Invoice_add_month", add_months(col("InvoiceDate"), 2))
       .withColumn("Invoice_add_date", date_add(col("InvoiceDate"), 30))
       .withColumn("Invoice_sub_date", date_sub(col("InvoiceDate"), 30))
       .withColumn("Invoice_datediff", datediff(current_date(),col("InvoiceDate")))
       .withColumn("InvoiceDateQuarter", quarter(col("InvoiceDate")))
       .withColumn("Invoice_id", unix_timestamp(col("InvoiceDate")))
       .withColumn("Invoice_format", from_unixtime(unix_timestamp(col("InvoiceDate")), "dd-MM-yyyy"))
       .show(10)

     df_products.withColumn("productGp", substring(col("PRODUCTGROUPNAME"),2, 2))
       .withColumn("productLength", length(col("PRODUCTGROUPNAME")))
       .withColumn("concat_prod", concat_ws("|", col("PRODUCTID"), col("INSTOCKFLAG")))
       .withColumn("PRODUCTGROUPCODEMIN", lower(col("PRODUCTGROUPCODE")))
       //.where(regexp_extract(trim(col("PRODUCTID")), "[0-9]{9}", 0) === trim(col("PRODUCTID")))
       .where(! col("PRODUCTID").rlike("[0-9]{5}"))
       .show(10)



     def valid_phone(phone_to_test : String): Boolean = {
       var result : Boolean = false
       val motif_regex = "^0[0-9]{9}".r
       if(motif_regex.findAllIn(phone_to_test.trim)== phone_to_test.trim){
         result = true
       } else {
         result =false
       }
       return result

     }
     val valid_phone_udf : UserDefinedFunction= udf{(phone_to_test: String)=> valid_phone(phone_to_test: String)}
     import session_s.implicits._
     val phone_list:  DataFrame = List("979809213", "+330989213476", "0627892134099").toDF("phones_numbers")
     phone_list.withColumn("test_phone", valid_phone_udf(col("phones_numbers")))
     .show()

   }

  def manip_rdd(): Unit = {

    val sc = SessionSpark(Env = true).sparkContext
    val session_s = SessionSpark(Env = true)

    sc.setLogLevel("OFF")
    val rdd_test : RDD[String]=sc.parallelize(List("alain", "juvenal", "anna"))
    rdd_test.foreach{
      l => println(l)
    }

    val rdd_2 : RDD[String] = sc.parallelize(Array("Lucie", "Fabien", "Jules"))
    rdd_2.foreach{l => println(l)}

    val  rdd_3 = sc.parallelize(Seq(("Julien", "Math", 15), ("Aline", "Math", 17), ("Juvenal", "Math", 19 )))
    println("Premier élément de mon RDD 3")
    rdd_3.take(1).foreach(l => println(l))

    if (rdd_3.isEmpty()){
      println("Le RDD est vide.")
    }
    else {
      rdd_3.foreach{l => println(l)}
    }

    //rdd_3.saveAsTextFile("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\texte\\RDD.txt")
    //rdd_3.repartition(1).saveAsTextFile("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\texte\\partition.txt")
    rdd_3.collect().foreach(l => println(l))

    //Creation d'un RDD  à partir d'une source de données grâce au RDD.textfile()
    val rdd_4 = sc.textFile("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Sources\\texteRDD.txt")
    //println("Lecture du contenu du RDD4")
    //rdd_4.foreach{l => println(l)}

    val rdd_5 = sc.textFile("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\Sources\\*")
    //println("Lecture du contenu du RDD5")
    //rdd_5.foreach{l => println(l)}

    // transformation des RDD
    val rdd_trans : RDD[String] = sc.parallelize(List("alain mange une banane", "la banane est bonne pour la santé", "acheter une bonne banane"))
    rdd_trans.foreach(l => println("ligne de mon RDD:  "+l))
    val rdd_map = rdd_trans.map(x => x.split(" "))
    //println("Nombre d'éléments de mon RDD map: "+ rdd_map.count())

    val rdd_6 = rdd_trans.map(w => (w, w.length, w.contains("banane"))).map(x=> (x._1.toLowerCase, x._2, x._3))


    val rdd_7 = rdd_6.map(x => (x._1.toUpperCase(), x._3, x._2))

    val  rdd_8 = rdd_6.map(x => (x._1.split(" "), 1))
    //rdd_8.foreach(l => println(l))

    val rdd_fm = rdd_trans.flatMap(x => x.split(" ")).map(w => (w, 1))
    //rdd_fm.foreach(l => println(l))

    val rdd_compte = rdd_5.flatMap(x => x.split(",")).flatMap(x => x.split(" "))

    //rdd_compte.repartition(1).saveAsTextFile("C:\\Users\\GZXZ4115\\Documents\\Juvenal Spark\\texte\\RDDComptage.txt")
    val  rdd_filter = rdd_fm.filter(x => x._1.contains("Banane"))

    val rdd_filter2 = rdd_2.filter( l => l.startsWith("L"))
    println("debut")


    val rdd_reduce = rdd_fm.reduceByKey((x,y) => x+y)
    rdd_reduce.foreach(l => println(l))

    import  session_s.implicits._
    val df :  DataFrame = rdd_fm.toDF("texte", "valeur")
    df.show()




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
          //.enableHiveSupport()
          .getOrCreate()
    } else {
      ss = SparkSession.builder().appName("Mon application Spark")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled", "true")
        // .enableHiveSupport()
        .getOrCreate()

    }
   ss

  }


}
