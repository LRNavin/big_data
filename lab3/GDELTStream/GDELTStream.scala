package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.Optional
import java.util.Date;
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.KeyValue

import scala.collection.JavaConversions._

object GDELTStream extends App {
  import Serdes._
  println("*********** Starting *************")
  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // create persistent KeyValueStore
  val countStoreSupplier = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("Counts"),
                                                       Serdes.String, Serdes.Long).withLoggingDisabled()
  builder.addStateStore(countStoreSupplier)

  val fullLogStoreSupplier = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("Logs"),
                                                          Serdes.String, Serdes.String).withLoggingDisabled()
  builder.addStateStore(fullLogStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  println("============== Filtering Stream ==============")
  val allNames = records.mapValues((key, value) => value.split("\t"))
    .filter((key, value) => value.length > 23)
    .flatMapValues((key, value) => value(23).split(",[0-9;]+").filter(_ != ""))

  println("*********** Calling Transformer & Posting to Viz *************")
  val transformed = allNames.transform(() => new HistogramTransformer(), "Counts", "Logs").to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

}

// This transformer should count the number of times a name occurs
// during the last hour. This means it needs to be able to
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, KeyValue[String, Long]] {
  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, Long] = _ // Histogram
  var fullLog: KeyValueStore[String, String] = _ // Windowed log of all messages

  // Change to one hour or as per manual.pdf
  val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val TimeWindowWidth = 10 // 3600 secs for one hour
  val HistogramRefreshRate = 10 // 3600 secs for one hour

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    println("*********** INIT *************")
    this.context = context
    this.kvStore = context.getStateStore("Counts").asInstanceOf[KeyValueStore[String, Long]]
    this.fullLog = context.getStateStore("Logs").asInstanceOf[KeyValueStore[String, String]]

    // Updates the window of the histogram in the specified interval
    this.context.schedule(this.HistogramRefreshRate, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {
      computeHistogram()
    })
  }

  def computeHistogram() = {
    println("Recomputing histogram ...")
    val recordsIterator = fullLog.all()
    var count = 0
    val currentDate: Date  = df.parse(df.format(Calendar.getInstance().getTime()))
    while(recordsIterator.hasNext){
      val record = recordsIterator.next()
      try{ // dealing with records comming with different formats
        val recordTimestamp = record.key.split("---")(0).toLong
        val recordDate: Date  = new Date(recordTimestamp)
        if((currentDate.getTime()  - recordDate.getTime()) / 1000 > this.TimeWindowWidth){
          println(record.value + " OUT!")
          kvStore.put(record.value, kvStore.get(record.value)-1)
          fullLog.delete(record.key)
          context.forward(record.value, kvStore.get(record.value))
        }
      } catch {
        case e: Exception => {

          println(record)
          println(e)
        }
      }
    }
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): KeyValue[String, Long] = {
    println("*********** TRANSFORMER CALLED *************")

    val recordDate: Date  = new Date(context.timestamp)
    val currentDate: Date  = df.parse(df.format(Calendar.getInstance().getTime()))

    val currentCount: Optional[Long] = Optional.ofNullable(kvStore.get(name))
    var incrementedCount: Long = currentCount.orElse(1L)

    println("record date: " + recordDate)
    println("current date: " + currentDate)

    if ((currentDate.getTime() - recordDate.getTime()) / 1000 < this.TimeWindowWidth) {
      fullLog.put(recordDate.getTime() + "---" + name, name)
      incrementedCount = incrementedCount + 1
    }

    if (incrementedCount < 0) incrementedCount = 0 // Temporary fix for the negative values that appear sometimes

    kvStore.put(name, incrementedCount)

    // return current count of the name during the last hour
    println(" -- Transformer Returns -- ")
    println(name + " - " + incrementedCount)

    (name, incrementedCount)

  }

  // Close any resources if any
  def close() {}
}