package kafka.logaccess

import java.util.Properties
import java.time.Duration
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import ml.combust.mleap.core.types._
import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import resource._

object LogAccessModel {

    val schema: StructType = StructType(
        StructField("total_count", ScalarType.Double),
        StructField("request_time_mean", ScalarType.Double),
        StructField("daily_counts", ScalarType.Double),
        StructField("is_weekend_ratio", ScalarType.Double),
        StructField("td_mean", ScalarType.Double),
        StructField("td_max", ScalarType.Double)
    ).get

    val modelpath = getClass.getResource("/model").getPath

    val model = (
        for(bundle <- managed(BundleFile(s"jar:$modelpath"))) yield {
            bundle.loadMleapBundle().get
        }
    ).tried.get.root

    def score(
        total_count: Double, request_time_mean: Double,
        daily_counts: Double, td_mean: Double,
        is_weekend_ratio: Double, td_max: Double
    ): Integer = {

        model.transform(
            DefaultLeapFrame(
                schema, 
                Seq(Row(total_count, request_time_mean, daily_counts, is_weekend_ratio, td_mean, td_max))
            )
        ).get.select("prediction").get.dataset.map(_.getInt(0)).head
    
    }

}


object LogAccessStreamClassifier extends App {

    import org.apache.kafka.streams.scala.Serdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    val config: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "logaccess-classifier")
        val bootstrapServers = if (args.length > 0) args(0) else "kafka:9092"
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        p
    }

    def logaccessStreamClassifier(
        inputTopic: String, outputTopic: String
    ): Topology = {

        val builder: StreamsBuilder = new StreamsBuilder()
        val logaccessInput = builder.stream[String, String](inputTopic)
        val logaccessScore: KStream[String, String] = logaccessInput.map(
            (_, value) => {
                val logaccess_values = value.split(",").map(_.toDouble)
                ("", Seq(value, LogAccessModel.score(logaccess_values(0), logaccess_values(1), logaccess_values(2), 
                logaccess_values(3), logaccess_values(4), logaccess_values(5))).mkString(","))
            }
        )
        logaccessScore.to(outputTopic)
        builder.build()
    }

    val streams: KafkaStreams = new KafkaStreams(
        logaccessStreamClassifier(
            "logaccess-classifier-input",
            "logaccess-classifier-output"
        ), config
    )
    streams.start()

    sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
    }

}
