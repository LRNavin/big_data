package gdelt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray



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
        StructField("GKGRECORDID", StringType, nullable=true),
        StructField("DATE", TimestampType,  nullable=true),
        StructField("SourceCollectionIdentifier", IntegerType,  nullable=true),
        StructField("SourceCommonName", StringType, nullable=true),
        StructField("DocumentIdentifier", StringType, nullable=true),
        StructField("Counts", StringType, nullable=true),
        StructField("V2Counts", StringType, nullable=true),
        StructField("Themes", StringType, nullable=true),
        StructField("V2Themes", StringType, nullable=true),
        StructField("Locations", StringType, nullable=true),
        StructField("V2Locations", StringType, nullable=true),
        StructField("Persons", StringType, nullable=true),
        StructField("V2Persons", StringType, nullable=true),
        StructField("Organizations", StringType, nullable=true),
        StructField("V2Organizations", StringType, nullable=true),
        StructField("V2Tone", StringType, nullable=true),
        StructField("Dates", StringType, nullable=true),
        StructField("GCAM", StringType, nullable=true),
        StructField("SharingImage", StringType, nullable=true),
        StructField("RelatedImages", StringType, nullable=true),
        StructField("SocialImageEmbeds", StringType, nullable=true),
        StructField("SocialVideoEmbeds", StringType, nullable=true),
        StructField("Quotations", StringType, nullable=true),
        StructField("AllNames", StringType, nullable=true),
        StructField("Amounts", StringType, nullable=true),
        StructField("TranslationInfo", StringType, nullable=true),
        StructField("Extras", StringType, nullable=true)
      )
    )

  case class GdeltData (
                         GKGRECORDID: String,
                         DATE: Timestamp,
                         SourceCollectionIdentifier: Integer,
                         SourceCommonName: String,
                         DocumentIdentifier: String,
                         Counts: String,
                         V2Counts: String,
                         Themes: String,
                         V2Themes: String,
                         Locations: String,
                         V2Locations: String,
                         Persons: String,
                         V2Persons: String,
                         Organizations: String,
                         V2Organizations: String,
                         V2Tone: String,
                         Dates: String,
                         GCAM: String,
                         SharingImage: String,
                         RelatedImages: String,
                         SocialImageEmbeds: String,
                         SocialVideoEmbeds: String,
                         Quotations: String,
                         AllNames: String,
                         Amounts: String,
                         TranslationInfo: String,
                         Extras: String
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

    val ds = sc.textFile(folderPath).map(line => line.split("\t"))

    val filtered = ds.filter(row => row.length > 23)

    val data = filtered.map(record => ((record(1)), (record(23).split(";"))))

    val formatted = data.map {case (left, right) => ( left.take(8), right.map(x => x.split(",")(0)))} // (date, Array(word))

    val flat = formatted.flatMapValues(x => x) // (date, word)

    val reduced = (flat.map { case (left, right) => ((left, right), 1) }).reduceByKey(_+_) // ((date, word), count)

    val perDay = reduced.map{ case ( (x,y),z ) => (x ,(y,z) )}.groupByKey()

    val sorted = perDay.mapValues(x => x.toList.sortBy(x => -x._2).take(10))

    sorted.collect().foreach(println)

    spark.stop
  }

  def DataFrame_Implementation(folderPath:String, detailedPrinting: Boolean){

    println("IMPLEMENTATION: DATASET")
    println(folderPath)

    val df = spark
      .read
      .schema(schema)
      .option("sep", "\t")
      .option("timestampFormat", "yyyyMMddHHmmss")
      .csv(folderPath)

    val ds = df
      .select($"DATE", $"AllNames")
      .as[GdeltDataReduced]
      .withColumn("DATE", date_trunc("dd", $"DATE"))
      .withColumn("AllNames", split($"AllNames",";"))
      .filter("Date is not null")
      .filter("AllNames is not null")
      .withColumn("topic", explode($"AllNames"))
      .withColumn("topic", regexp_replace($"topic", ",[0-9]+", ""))
      .select("DATE", "topic")

    if (detailedPrinting){
      println("=============================")
      println("== GENERAL INFO: FULL DATA ==")
      println("Number of records of Dataset: " + ds.count())
      ds.printSchema()
      ds.show(10)
      println("=============================")
    }

    val top_ds = ds
      .groupBy($"DATE", $"topic")
      .agg(count($"topic").as("count"))
      .withColumn("topicCountsTuples", intoTuple($"topic",$"count"))
      .groupBy("DATE")
      .agg(collect_list("topicCountsTuples").as("topicCountsTuples"))
      .withColumn("topTenTopics", getTopTenUDF($"topicCountsTuples"))
      .select("DATE", "topTenTopics")

    println("=============================")
    println("=== GENERAL INFO: TOP TEN ===")
    println("Number of records of Dataset: " + top_ds.count())
    top_ds.printSchema()
    top_ds.show(10)
    println("=============================")

    top_ds.take(1).foreach(r => println(r.get(1)))

    spark.stop
  }

  var dataset_size = "1"
  var data_path = "data/".concat("segment_").concat(dataset_size)
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