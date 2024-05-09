import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamNBAStats {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("hsiangct_final_NBA_stats"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamNBAStats")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("hsiangct_final_NBA")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[NBAStats]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val put = new Put(Bytes.toBytes(wr.key))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("player_name"), Bytes.toBytes(wr.player_name))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("season"), Bytes.toBytes(wr.season))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(wr.age))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pts"), Bytes.toBytes(wr.pts))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("reb"), Bytes.toBytes(wr.reb))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ast"), Bytes.toBytes(wr.ast))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("college"), Bytes.toBytes(wr.college))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("draft_year"), Bytes.toBytes(wr.draft_year))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("draft_round"), Bytes.toBytes(wr.draft_round))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("draft_number"), Bytes.toBytes(wr.draft_number))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gp"), Bytes.toBytes(wr.gp))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("net_rating"), Bytes.toBytes(wr.net_rating))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("oreb_pct"), Bytes.toBytes(wr.oreb_pct))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("dreb_pct"), Bytes.toBytes(wr.dreb_pct))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("usg_pct"), Bytes.toBytes(wr.usg_pct))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ts_pct"), Bytes.toBytes(wr.ts_pct))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ast_pct"), Bytes.toBytes(wr.ast_pct))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("country"), Bytes.toBytes(wr.country))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("player_height"), Bytes.toBytes(wr.player_height))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("player_weight"), Bytes.toBytes(wr.player_weight))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("team_abbreviation"), Bytes.toBytes(wr.team_abbreviation))

      table.put(put)
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
