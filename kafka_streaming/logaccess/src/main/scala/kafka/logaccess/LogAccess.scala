package kafka.iris

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



object IrisModel {

    val schema: StructType = StructType(
        StructField("sepal_length_cm", ScalarType.Double),
        StructField("sepal_width_cm", ScalarType.Double),
        StructField("petal_length_cm", ScalarType.Double),
        StructField("petal_width_cm", ScalarType.Double),
        StructField("class", ScalarType.String)
    ).get

    val modelpath = getClass.getResource("/model").getPath

    val model = (
        for(bundle <- managed(BundleFile(s"jar:$modelpath"))) yield {
            bundle.loadMleapBundle().get
        }
    ).tried.get.root

    def score(
        sepal_length_cm: Double, sepal_width_cm: Double,
        petal_length_cm: Double, petal_width_cm: Double
    ): String = {

        model.transform(
            DefaultLeapFrame(
                schema, 
                Seq(Row(sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, "Iris-setosa"))
            )
        ).get.select("predictedLabel").get.dataset.map(_.getString(0)).head
    
    }

}


object IrisStreamClassifier extends App {

    import org.apache.kafka.streams.scala.Serdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    val config: Properties = {
        val p = new Properties()
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, "iris-classifier")
        val bootstrapServers = if (args.length > 0) args(0) else "kafka:9092"
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        p
    }

    def irisStreamClassifier(
        inputTopic: String, outputTopic: String
    ): Topology = {

        val builder: StreamsBuilder = new StreamsBuilder()
        val irisInput = builder.stream[String, String](inputTopic)
        val irisScore: KStream[String, String] = irisInput.map(
            (_, value) => {
                val iris_values = value.split(",").map(_.toDouble)
                ("", Seq(value, IrisModel.score(iris_values(0), iris_values(1), iris_values(2), iris_values(3))).mkString(","))
            }
        )
        irisScore.to(outputTopic)
        builder.build()
    }

    val streams: KafkaStreams = new KafkaStreams(
        irisStreamClassifier(
            "iris-classifier-input",
            "iris-classifier-output"
        ), config
    )
    streams.start()

    sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
    }

}
