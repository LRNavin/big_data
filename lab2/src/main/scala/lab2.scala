package gdelt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray
import java.sql.Date
import scala.annotation.switch



object Lab2{

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


  def RDD_Implementation(folderPath: String){

    println("IMPLEMENTATION: RDD")
    val data = sc.textFile(folderPath);
    
    val t1 = System.nanoTime

    val ds = data.map(line => line.split("\t"))
            .filter(row => row.length > 23)
            .map(record => ((record(1)), (record(23).split(";"))))
            .map {case (left, right) => ( left.take(8), right.map(x => x.split(",")(0)))} // (date, Array(word))
            .flatMapValues(x => x)  // (date, word)
            .filter(!_._2.contains("Type ParentCategory"))
            .map { case (left, right) => ((left, right), 1) }.reduceByKey(_+_) // ((date, word), count)
            .map{ case ( (x,y),z ) => (x ,(y,z) )}.groupByKey() // ((date, word), count) => (date, (word, count))
            .mapValues(x => x.toList.sortBy(x => -x._2).take(10)) //sort by count and take top ten

    println("==================================================")
    println("=== O/P TOP TEN OCCURENCES===")
    println("**Word\tcount**")
    ds.map { case (a, list) => list } // "flatten" into single array
    .map(_.mkString("\n")) // combine into Tab-separated string
    .foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("******************* Elapsed Time *******************")
    println(duration)
    spark.stop
  }

  def DataFrame_Implementation(folderPath:String, details: Boolean){

    println("IMPLEMENTATION: DATASET")
    val df = spark
      .read
      .schema(schema)
      .option("sep", "\t")
      .option("timestampFormat", "yyyyMMddHHmmss")
      .csv(folderPath).as[GdeltData]
    
    val t1 = System.nanoTime

    val ds = df
      .withColumn("date", date_trunc("dd", $"date"))
      .withColumn("allNames", split($"allNames",";"))
      .filter("date is not null")
      .filter("allNames is not null")
      .withColumn("topic", explode($"allNames"))
      .withColumn("topic", regexp_replace($"topic", ",[0-9]+", ""))
      .select("date", "topic").as[(Timestamp, String)]

    // Not calling this for now
    if (details){
      println("=============================")
      println("== INFO: DATA SUMMARY ==")
      println("Dataset records count: " + ds.count())
      ds.printSchema()
      ds.show(5)
      println("=============================")
    }

    println("==================================================")
    println("=== O/P TOP TEN OCCURENCES ===")

    // WITH RANK
    // val top_ds = ds
    // .flatMap { case (x1, x2) => x2.replaceAll("[,0-9]", "").split(";").map((x1, _))}
    // .filter(!_._2.equals("Type ParentCategory"))
    // .groupBy('_1, '_2 as "word").count
    // .withColumn("rank", rank.over(Window.partitionBy('_1).orderBy('count.desc)))
    // // Used rank to get top 10
    // .filter('rank <= 10)
    // // Drop the extra column rank
    // .drop("rank")
    // // Print the output
    // top_ds.show();
   
    // WITHOUT RANK
    val top_ds = ds
      .groupBy($"date", $"topic")
      .agg(count($"topic").as("count"))
      .withColumn("topicCountsTuples", intoTuple($"topic",$"count"))
      .groupBy("date")
      .agg(collect_list("topicCountsTuples").as("topicCountsTuples"))
      .withColumn("topTenTopics", getTopTenUDF($"topicCountsTuples")) //Will try rank here!
      .select("date", "topTenTopics")
    // print output
    top_ds.write.format("json").save("s3://anwesh-tud-aws/output_logs")
    // top_ds.take(1).foreach(r => println(r.get(1)))
    val duration = (System.nanoTime - t1) / 1e9d
    println("******************* Elapsed Time *******************")
    println(duration)

    spark.stop
  }

  def checkInit(){
    println("HELLO WORLD");
  }

  def main(args: Array[String]){

    println("WELCOME TO GDELT - Big Data Extractor")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if(args.length == 2){
      println("Extracting data from GDELT size: " + args(1) + ", compute using: " + args(0))
      println("********************")
      // Used switch cases to have one .jar with argument combinations
      // Run using spark-submit <algo-type> <data-size>
      // Where, Algo-type = rdd/dataset; data-size = s(day), m(1 month) ,l(6 month), f(FULL), l(LOCAL), hello(check)
      args(0) match {
        case "dataset" => {
          args(1) match{
            case "s" => {
              DataFrame_Implementation("s3://gdelt-open-data/v2/gkg/20150218230000.gkg.csv", false)
            }
            case "m" => {
              //one month
              DataFrame_Implementation("s3n://gdelt-open-data/v2/gkg/201502*", false)
            }
            case "l" => {
              //6 months
              DataFrame_Implementation("s3://gdelt-open-data/v2/gkg/20150[1-6]*", false)
            }
            case "f" => {
              DataFrame_Implementation("s3://gdelt-open-data/v2/gkg/*", false)
            }
            case "local" => {
              DataFrame_Implementation("./data/20150218230000.gkg.csv", false)
            }
            case "hello" => {
              checkInit();
            }
            case _ => {
              println("Wrong argument for Dataset size")
            }
          }
        }
      case "rdd" => {
          args(1) match{
            case "s" => {
              RDD_Implementation("s3://gdelt-open-data/v2/gkg/20150218230000.gkg.csv")
            }
            case "m" => {
              //one month
              RDD_Implementation("s3://gdelt-open-data/v2/gkg/201502*")
            }
            case "l" => {
              //6 months
              RDD_Implementation("s3://gdelt-open-data/v2/gkg/20150[1-6]*")
            }
            case "f" => {
              RDD_Implementation("s3://gdelt-open-data/v2/gkg/*")
            }
            case "local" => {
              RDD_Implementation("./data/20150218230000.gkg.csv")
            }
            case "hello" => {
              checkInit();
            }
            case _ => {
              println("Wrong argument for RDD size")
            }
          }
        }
        case _ =>{
          println("Type not specified")
        } 
      }
    }
    else {
      println("Insufficient Arguments")
    }
    
    println("=======================")
    println("Done!")

    spark.stop()
    sc.stop()

  }

}