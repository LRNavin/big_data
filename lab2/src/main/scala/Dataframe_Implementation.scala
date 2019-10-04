package gdelt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray



object Lab2Implementations{

  // Spark initialization

  val spark = SparkSession
    .builder()
    .appName("GDELThist")
    //.config("spark.master", "local")
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


  def RDDImp(folderPath: String, detailedPrinting: Boolean){

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

  def DataframeImp(folderPath:String, detailedPrinting: Boolean){

    println("IMPLEMENTATION: DATASET")

    val df = spark
      .read
      .schema(schema)
      .option("sep", "\t")
      .option("timestampFormat", "yyyyMMddHHmmss")
      .csv(folderPath)

    val ds = df
      .select($"date", $"allNames")
      .as[GdeltDataReduced]
      .withColumn("date", date_trunc("dd", $"date"))
      .withColumn("allNames", split($"allNames",";"))
      .filter("date is not null")
      .filter("allNames is not null")
      .withColumn("topic", explode($"allNames"))
      .withColumn("topic", regexp_replace($"topic", ",[0-9]+", ""))
      .select("date", "topic")

    if (detailedPrinting){
      println("=============================")
      println("== GENERAL INFO: FULL DATA ==")
      println("Number of records of Dataset: " + ds.count())
      ds.printSchema()
      ds.show(10)
      println("=============================")
    }

    val top_ds = ds
      .groupBy($"date", $"topic")
      .agg(count($"topic").as("count"))
      .withColumn("topicCountsTuples", intoTuple($"topic",$"count"))
      .groupBy("date")
      .agg(collect_list("topicCountsTuples").as("topicCountsTuples"))
      .withColumn("topTenTopics", getTopTenUDF($"topicCountsTuples"))
      .select("date", "topTenTopics")

    if (detailedPrinting){
      println("=============================")
      println("=== GENERAL INFO: TOP TEN ===")
      println("Number of records of Dataset: " + top_ds.count())
      top_ds.printSchema()
      top_ds.show(10)
      println("=============================")
    }

    //top_ds.take(1).foreach(r => println(r.get(1)))

    // top_ds.foreach(r => println(r))
    top_ds.show()

    // top_ds.write.json("~/result/" + System.currentTimeMillis/1000) // change with s3 path

    //spark.stop
  }

  def simpleImplementation(){
    println("CLUSTER WORKING: HELLO WORLD");
  }

  def main(args: Array[String]){

    println("WELCOME TO GDELT")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if(args.length == 2){

      println("CONFIG: " + args(0) + ", " + args(1))
      println("=======================")
      if(args(1) == "dataset"){
        if(args(0) == "small"){
          DataframeImp("s3://gdelt-open-data/v2/gkg/20150218230000.gkg.csv", false)
        }else if(args(0) == "medium"){
          DataframeImp("s3://gdelt-open-data/v2/gkg/201502*", false)
        }else if(args(0) == "large"){
          DataframeImp("s3://gdelt-open-data/v2/gkg/20150[1-6]*", false)
        }else if(args(0) == "xlarge"){
          DataframeImp("s3://gdelt-open-data/v2/gkg/2015*", false)
        }else if(args(0) == "full"){
          DataframeImp("s3://gdelt-open-data/v2/gkg/*", false)
        }else if(args(0) == "hello"){
          simpleImplementation()
        }else if(args(0) == "local"){
          DataframeImp("segment/1_segment", false)
        }else{
          println("wrong argument")
        }
      }else if(args(1) == "rdd"){
        if(args(0) == "small"){
          RDDImp("s3://gdelt-open-data/v2/gkg/20150218230000.gkg.csv", false)
        }else if(args(0) == "medium"){
          RDDImp("s3://gdelt-open-data/v2/gkg/201502*", false)
        }else if(args(0) == "large"){
          RDDImp("s3://gdelt-open-data/v2/gkg/20150[1-6]*", false)
        }else if(args(0) == "xlarge"){
          RDDImp("s3://gdelt-open-data/v2/gkg/2015*", false)
        }else if(args(0) == "full"){
          RDDImp("s3://gdelt-open-data/v2/gkg/*", false)
        }else if(args(0) == "hello"){
          simpleImplementation()
        }else if(args(0) == "local"){
          RDDImp("segment/1_segment", false)
        }else{
          println("wrong argument")
        }
      }
    }else{
      println("NEED TO PROVIDE THE SIZE TO TRY AS AN ARGUMENT (small,medium, large, full, hello)")
    }

    println("=======================")
    println("GOODBYE!")

    spark.stop()
    sc.stop()

  }

}