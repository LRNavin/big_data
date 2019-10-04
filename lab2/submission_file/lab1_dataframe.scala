package gdelt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import java.sql.Date

object Lab1{

  // Spark initialization

  val spark = SparkSession
    .builder()
    .appName("GDELThist")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  // Stuff used in the Dataset implementation

  val schema =
    StructType(
      Array(
        StructField("gkgrecordid", StringType, nullable=true),
        StructField("date", TimestampType,  nullable=true),
        StructField("sourceCollectionIdentifier", IntegerType,  nullable=true),
        StructField("sourceCommonName", StringType, nullable=true),
        StructField("documentIdentifier", StringType, nullable=true),
        StructField("counts", StringType, nullable=true),
        StructField("v2Counts", StringType, nullable=true),
        StructField("themes", StringType, nullable=true),
        StructField("v2Themes", StringType, nullable=true),
        StructField("locations", StringType, nullable=true),
        StructField("v2Locations", StringType, nullable=true),
        StructField("persons", StringType, nullable=true),
        StructField("v2Persons", StringType, nullable=true),
        StructField("organizations", StringType, nullable=true),
        StructField("v2Organizations", StringType, nullable=true),
        StructField("v2Tone", StringType, nullable=true),
        StructField("dates", StringType, nullable=true),
        StructField("gcam", StringType, nullable=true),
        StructField("sharingImage", StringType, nullable=true),
        StructField("relatedImages", StringType, nullable=true),
        StructField("socialImageEmbeds", StringType, nullable=true),
        StructField("socialVideoEmbeds", StringType, nullable=true),
        StructField("quotations", StringType, nullable=true),
        StructField("allNames", StringType, nullable=true),
        StructField("amounts", StringType, nullable=true),
        StructField("translationInfo", StringType, nullable=true),
        StructField("extras", StringType, nullable=true)
      )
    )

  case class GdeltData (
                         gkgrecordid: String,
                         date: Timestamp,
                         sourceCollectionIdentifier: Integer,
                         sourceCommonName: String,
                         documentIdentifier: String,
                         counts: String,
                         v2Counts: String,
                         themes: String,
                         v2Themes: String,
                         locations: String,
                         v2Locations: String,
                         persons: String,
                         v2Persons: String,
                         organizations: String,
                         v2Organizations: String,
                         v2Tone: String,
                         dates: String,
                         gcam: String,
                         sharingImage: String,
                         relatedImages: String,
                         socialImageEmbeds: String,
                         socialVideoEmbeds: String,
                         quotations: String,
                         allNames: String,
                         amounts: String,
                         translationInfo: String,
                         extras: String
                       )

  case class GdeltDataReduced (
                                date: Timestamp,
                                allNames: String
                              )

  // UDFs for operating on columns in Spark (Dataset implementation)

  val intoTuple = udf((a: String, b: Int) => (a, b))

  val getTopTenUDF = udf { arr: WrappedArray[Row] =>
    arr.map(r => (r.getAs[String](0), r.getAs[Int](1))).sortWith(_._2 > _._2).take(10)
  }


  def RDD_Implementaion(folderPath: String, detailedPrinting: Boolean){

    println("IMPLEMENTATION: RDD")
    val t1 = System.nanoTime
    val ds = sc.textFile(folderPath).map(line => line.split("\t"))
            .filter(row => row.length > 23)
            .map(record => ((record(1)), (record(23).split(";"))))
            .map {case (left, right) => ( left.take(8), right.map(x => x.split(",")(0)))} // (date, Array(word))
            .flatMapValues(x => x)  // (date, word)
            .filter(!_._2.contains("Type ParentCategory"))
            .map { case (left, right) => ((left, right), 1) }.reduceByKey(_+_) // ((date, word), count)
            .map{ case ( (x,y),z ) => (x ,(y,z) )}.groupByKey()
            .mapValues(x => x.toList.sortBy(x => -x._2).take(10))

    ds.foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("******************* Elapsed Time *******************")
    println(duration)
    spark.stop
  }

  def DataFrame_Implementation(folderPath:String, detailedPrinting: Boolean){

    println("IMPLEMENTATION: DATASET")
    val t1 = System.nanoTime
    val df = spark
      .read
      .schema(schema)
      .option("sep", "\t")
      .option("timestampFormat", "yyyyMMddHHmmss")
      .csv(folderPath).as[GdeltData]

    val ds = df
      .withColumn("date", date_trunc("dd", $"date"))
      .withColumn("allNames", split($"allNames",";"))
      .filter("date is not null")
      .filter("allNames is not null")
      .withColumn("topic", explode($"allNames"))
      .withColumn("topic", regexp_replace($"topic", ",[0-9]+", ""))
      .select("date", "topic")

    val top_ds = ds
      .groupBy($"date", $"topic")
      .agg(count($"topic").as("count"))
      .withColumn("topicCountsTuples", intoTuple($"topic",$"count"))
      .groupBy("date")
      .agg(collect_list("topicCountsTuples").as("topicCountsTuples"))
      .withColumn("topTenTopics", getTopTenUDF($"topicCountsTuples"))
      .select("date", "topTenTopics")

    println("=============================")
    println("=== GENERAL INFO: TOP TEN ===")
    top_ds.take(1).foreach(r => println(r.get(1)))
    val duration = (System.nanoTime - t1) / 1e9d
    println("******************* Elapsed Time *******************")
    println(duration)
    println("=============================")

    spark.stop
  }

//  var dataset_size = "1"
  var data_path = "data/".concat("segment") //.concat(dataset_size)
  var algo = "DataFrame"  // "RDD" or "DataFrame"

  def main(args: Array[String]){

    println("WELCOME TO GDELT - Big Data Extractor")
    println("Extracting data from -- ".concat(data_path).concat(" ......"))
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if(algo == "RDD"){
      RDD_Implementaion(data_path, true)
    }
    else{
      DataFrame_Implementation(data_path, false)
    }

    spark.stop()

  }

}
