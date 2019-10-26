package lab3

import java.util.Properties
import java.util.Optional
import java.util.Date;
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit


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

  val logStoreSupplier = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("Logs"),
                                                          Serdes.String, Serdes.String).withLoggingDisabled()
  builder.addStateStore(logStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  println("============== Filtering Stream ==============")
  val filteredData = records.mapValues((key, value) => value.split("\t")) // Split stream record with tabs
      .filter((key, value) => value.length > 23) // Remove corrupted data
      .flatMapValues((key, value) => value(23) // Flatten the list
      .split(",[0-9;]+") // Get list of names
      .filter(_ != "")) // Remove empty names

  println("*********** Calling Transformer & Posting to Viz *************")
  // Perform the histogram transformation and post it to "gdelt-histogram" topic for visualization
  val transformed = filteredData.transform(() => new HistogramTransformer(), "Counts", "Logs").to("gdelt-histogram")

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
  var countStore: KeyValueStore[String, Long] = _ // Histogram
  var logStore: KeyValueStore[String, String] = _ // Windowed log of all messages

  // Change to 'x' hour or as per manual.pdf
  val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val timeWindow = 3600 // 3600 secs for one hour
  val refreshFrequency = 30 // 3600 secs for one hour

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    println("*********** INIT *************")
    this.context = context
    // Retrive persisitent stores - count, log before transforming
    this.countStore = context.getStateStore("Counts").asInstanceOf[KeyValueStore[String, Long]]
    this.logStore = context.getStateStore("Logs").asInstanceOf[KeyValueStore[String, String]]

    // Updates the window of the histogram in the specified interval
    this.context.schedule(this.refreshFrequency, PunctuationType.WALL_CLOCK_TIME, (timestamp) => {
      computeHistogram()
    })
  }

  def computeHistogram() = {
    println("Refreshing Data Store ->->->")
    // Refresh Histogram Periodically (every refreshFrequency) - By Updating Name Store using Log Store
    val recordsIterator = logStore.all() // To iterate over all logs
    val newDate: Date  = df.parse(df.format(Calendar.getInstance().getTime()))

    // Iterationg over all exisitng logs
    while(recordsIterator.hasNext){
      val record = recordsIterator.next()
      try{
        val recordTimestamp = record.key.split("-")(0).toLong
        val oldDate: Date  = new Date(recordTimestamp)
        // Time Checker - For Old data i.e Data Age > timeWindow
        if((newDate.getTime()  - oldDate.getTime()) / 1000 > this.timeWindow){
          // Decreament count of name state record
          countStore.put(record.value, countStore.get(record.value)-1)
          // Delete log of record
          logStore.delete(record.key)
          context.forward(record.value, countStore.get(record.value)) // Update
        }
      } catch {
        case e: Exception => {
          println("Warning Exception !!!!!!!!!")
          println(record)
          println(e)
        }
      }
    }
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): KeyValue[String, Long] = {
    println("*********** TRANSFORMER CALLED *************")

    val oldDate: Date  = new Date(context.timestamp)
    val newDate: Date  = df.parse(df.format(Calendar.getInstance().getTime()))

    val currentCount: Optional[Long] = Optional.ofNullable(countStore.get(name))
    // 1. If Null (New Name) - Initialize to 0 - Add a new record to the histogram
    var newCount: Long = currentCount.orElse(0L)

    // Calculate New name count w.r.t current time - Update Count & Update the logs with current time
    if ((newDate.getTime() - oldDate.getTime()) / 1000 < this.timeWindow) {
      logStore.put(oldDate.getTime() + "-" + name, name)
      //2. Increment count for the name record if it occurs again
      newCount = newCount + 1
    }

    // Update New name count in store
    if (newCount < 0) {
      newCount = 0 // Dirty fix for the negative values
    } else {
      countStore.put(name, newCount) // Count update
    }

    if (true) { // Toggle for prints
      println(" -- Transformer Returns -- ")
      println(name + " - " + newCount)
    }
    // current time window's - count of the name
    (name, newCount)
  }

  // Close any resources if any
  def close() {}
}