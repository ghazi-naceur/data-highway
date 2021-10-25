package gn.oss.data.highway.engine

import java.time.Duration
import java.util.UUID
import org.apache.kafka.streams.KafkaStreams
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import monix.execution.Scheduler.{global => scheduler}
import cats.implicits._
import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.models.{
  Cassandra,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwayResponse,
  Earliest,
  Elasticsearch,
  File,
  HDFS,
  JSON,
  Kafka,
  KafkaConsumer,
  Latest,
  Local,
  Offset,
  Output,
  PureKafkaConsumer,
  PureKafkaStreamsConsumer,
  SparkKafkaPluginConsumer,
  SparkKafkaPluginStreamsConsumer,
  Storage
}
import gn.oss.data.highway.utils.Constants.{SUCCESS, TRIGGER}
import gn.oss.data.highway.utils.DataFrameUtils.sparkSession
import gn.oss.data.highway.utils.{
  FilesUtils,
  HdfsUtils,
  KafkaTopicConsumer,
  KafkaUtils,
  SharedUtils
}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{struct, to_json}

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object KafkaSampler extends HdfsUtils {

  val logger: Logger = Logger.getLogger(KafkaSampler.getClass.getName)

  /**
    * Consumes data from a topic
    *
    * @param input The input Kafka entity
    * @param output The output File entity
    * @param storage The output file system storage
    * @return DataHighwayResponse, otherwise a DataHighwayErrorResponse
    */
  def consumeFromTopic(
      input: Kafka,
      output: Output,
      storage: Option[Storage],
      consistency: Option[Consistency]
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    val (temporaryPath, tempoBasePath) =
      SharedUtils.setTempoFilePath("kafka-sampler", storage)
    val consumer = input.kafkaMode.asInstanceOf[Option[KafkaConsumer]]
    val fsys     = SharedUtils.setFileSystem(output, storage)
    consumer match {
      case Some(km) =>
        KafkaUtils.verifyTopicExistence(input.topic, km.brokers, enableTopicCreation = false)
        km match {
          case PureKafkaConsumer(brokers, consGroup, offset) =>
            // Continuous job - to be triggered once
            val result =
              Either.catchNonFatal(scheduler.scheduleWithFixedDelay(0.seconds, 5.seconds) {
                sinkWithPureKafkaConsumer(
                  input.topic,
                  temporaryPath,
                  tempoBasePath,
                  output,
                  fsys,
                  consistency,
                  brokers,
                  offset,
                  consGroup,
                  fs
                )
              })
            SharedUtils.constructIOResponse(input, output, result, TRIGGER)
          case PureKafkaStreamsConsumer(brokers, streamAppId, offset) =>
            // Continuous job - to be triggered once
            val result = sinkWithPureKafkaStreamsConsumer(
              input.topic,
              temporaryPath,
              tempoBasePath,
              output,
              fsys,
              consistency,
              brokers,
              offset,
              streamAppId,
              fs
            )
            SharedUtils.constructIOResponse(input, output, result, TRIGGER)
          case SparkKafkaPluginConsumer(brokers, offset) =>
            // Batch/One-shot job - to be triggered everytime
            val result = Either.catchNonFatal {
              sinkWithSparkKafkaConnector(
                sparkSession,
                input,
                output,
                temporaryPath,
                tempoBasePath,
                fsys,
                consistency,
                brokers,
                offset
              )
            }
            SharedUtils.constructIOResponse(input, output, result, SUCCESS)
//          case SparkKafkaPluginStreamsConsumer(brokers, offset) =>
//            // only json data type support
//            Either.catchNonFatal {
//              val thread = new Thread(() => {
//                sinkWithSparkKafkaStreamsConnector(
//                  sparkSession,
//                  input.topic,
//                  output.path,
//                  filesystem,
//                  brokers,
//                  offset
//                )
//              })
//              thread.start()
//            }
          case _ =>
            throw new RuntimeException(
              s"This mode is not supported while consuming Kafka topics. The supported modes are " +
                s"${PureKafkaConsumer.getClass}, ${SparkKafkaPluginConsumer.getClass}, ${PureKafkaStreamsConsumer.getClass}" +
                s"and ${SparkKafkaPluginStreamsConsumer.getClass}. The provided input kafka mode is '$km'."
            )
        }
    }
  }

  /**
    * Sinks topic content in output entity using a [[PureKafkaConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param temporaryPath The temporary output folder
    * @param tempoBasePath The base of the temporary output folder
    * @param output The output File entity
    * @param storage The output file system storage
    * @param consistency The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param consumerGroup The consumer group name
    * @param fs The output file system entity
    * @return Unit, otherwise a Throwable
    */
  private[engine] def sinkWithPureKafkaConsumer(
      inputTopic: String,
      temporaryPath: String,
      tempoBasePath: String,
      output: Output,
      storage: Storage,
      consistency: Option[Consistency],
      brokerUrls: String,
      offset: Offset,
      consumerGroup: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    KafkaTopicConsumer
      .consume(inputTopic, brokerUrls, offset, consumerGroup)
      .map(consumed => {
        val record = consumed.poll(Duration.ofSeconds(1)).asScala
        for (data <- record.iterator) {
          logger.info(
            s"Topic: ${data.topic()}, Key: ${data.key()}, Value: ${data.value()}, " +
              s"Offset: ${data.offset()}, Partition: ${data.partition()}"
          )
          val uuid: String =
            s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
          storage match {
            case Local =>
              FilesUtils
                .createFile(temporaryPath, s"simple-consumer-$uuid.${JSON.extension}", data.value())
            case HDFS =>
              HdfsUtils
                .save(fs, s"$temporaryPath/simple-consumer-$uuid.${JSON.extension}", data.value())
          }
          logger.info(
            s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
              s"'$temporaryPath/simple-consumer-*****${JSON.extension}'"
          )
        }
        consumed.close()
      })
    dispatchDataToOutput(
      temporaryPath,
      tempoBasePath,
      output,
      storage,
      consistency
    )
  }

  /**
    * Sinks topic content in output entity using a [[PureKafkaStreamsConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param temporaryPath The temporary output folder
    * @param tempoBasePath The base of the temporary output folder
    * @param output The output File entity
    * @param storage The output file system storage
    * @param consistency The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @param streamAppId The identifier of the streaming application
    * @param fs The output file system entity
    * @return Unit, otherwise a Throwable
    */
  private[engine] def sinkWithPureKafkaStreamsConsumer(
      inputTopic: String,
      temporaryPath: String,
      tempoBasePath: String,
      output: Output,
      storage: Storage,
      consistency: Option[Consistency],
      brokerUrls: String,
      offset: Offset,
      streamAppId: String,
      fs: FileSystem
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val kafkaStreamEntity =
        KafkaTopicConsumer.consumeWithStream(streamAppId, inputTopic, offset, brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(data => {
        val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
        storage match {
          case Local =>
            FilesUtils.createFile(temporaryPath, s"kafka-streams-$uuid.${JSON.extension}", data)
          case HDFS =>
            HdfsUtils.save(fs, s"$temporaryPath/kafka-streams-$uuid.${JSON.extension}", data)
        }
        logger.info(
          s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
            s"'$temporaryPath/kafka-streams-*****${JSON.extension}'"
        )
        dispatchDataToOutput(
          temporaryPath,
          tempoBasePath,
          output,
          storage,
          consistency
        )
      })
      val streams = new KafkaStreams(kafkaStreamEntity.builder.build(), kafkaStreamEntity.props)
      streams.start()
    }
  }

  /**
    * Sinks topic content in output entity using a [[SparkKafkaPluginConsumer]]
    *
    * @param session The Spark session
    * @param kafka The input Kafka entity
    * @param output The output File entity
    * @param temporaryPath The temporary output folder
    * @param tempoBasePath The base of the temporary output folder
    * @param storage The output file system storage
    * @param consistency The output save mode
    * @param brokerUrls The kafka brokers urls
    * @param offset The kafka consumer offset
    * @return Unit, otherwise a Throwable
    */
  def sinkWithSparkKafkaConnector(
      session: SparkSession,
      kafka: Kafka,
      output: Output,
      temporaryPath: String,
      tempoBasePath: String,
      storage: Storage,
      consistency: Option[Consistency],
      brokerUrls: String,
      offset: Offset
  ): Either[Throwable, Unit] = {
    import session.implicits._
    val computedOffset = offset match {
      case Latest =>
        logger.warn(
          s"Starting offset can't be '$offset' for batch queries on Kafka. So, we'll set it at 'Earliest'."
        )
        Earliest
      case Earliest =>
        offset
    }
    val frame = session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrls)
      .option("startingOffsets", computedOffset.value)
      .option("subscribe", kafka.topic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .select(to_json(struct("value")))
      .toJavaRDD

    frame
      .collect()
      .asScala
      .toList
      .traverse(data => {
        val line = data.toString.substring(11, data.toString().length - 3).replace("\\", "")
        storage match {
          case Local =>
            FilesUtils.createFile(
              temporaryPath,
              s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}",
              line
            )
          case HDFS =>
            HdfsUtils.save(
              fs,
              s"$temporaryPath/spark-kafka-plugin-${UUID.randomUUID()}-${System
                .currentTimeMillis()}.${JSON.extension}",
              line
            )
        }
      })
    dispatchDataToOutput(
      temporaryPath,
      tempoBasePath,
      output,
      storage,
      consistency
    )
  }

//  /**
//    * Sinks topic content into files using a [[io.oss.data.highway.models.SparkKafkaPluginStreamsConsumer]]
//    *
//    * @param session The Spark session
//    * @param inputTopic The input kafka topic
//    * @param storage The output file system storage
//    * @param brokerUrls The kafka brokers urls
//    * @param offset The kafka consumer offset
//    */
//  private[engine] def sinkWithSparkKafkaStreamsConnector(
//      session: SparkSession,
//      inputTopic: String,
//      outputPath: String,
//      storage: Storage,
//      brokerUrls: String,
//      offset: Offset
//  ): Unit = {
//    import session.implicits._
//    logger.info(
//      s"Starting to sink '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
//        s"'$outputPath/spark-kafka-streaming-plugin-*****${JSON.extension}'"
//    )
//    val stream = session.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", brokerUrls)
//      .option("startingOffsets", offset.value)
//      .option("subscribe", inputTopic)
//      .load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//      .select("value")
//      .writeStream
//      .format(JSON.extension)
//      .option("path", s"$outputPath/spark-kafka-streaming-plugin-${System.currentTimeMillis()}")
//    storage match {
//      case Local =>
//        stream
//          .option(
//            "checkpointLocation",
//            s"/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
//          )
//          .start()
//          .awaitTermination()
//      case HDFS =>
//        stream
//          .option(
//            "checkpointLocation",
//            s"${hadoopConf.host}/tmp/checkpoint/${UUID.randomUUID()}-${System.currentTimeMillis()}"
//          )
//          .start()
//          .awaitTermination()
//    }
//  }

  /**
    * Dispatches intermediate data to the provided output
    *
    * @param temporaryPath The temporary path
    * @param tempoBasePath The base of the temporary path
    * @param output The provided output entity
    * @param storage The file system storage
    * @param consistency The Spark save mode
    * @return Unit, otherwise a Throwable
    */
  private def dispatchDataToOutput(
      temporaryPath: String,
      tempoBasePath: String,
      output: Output,
      storage: Storage,
      consistency: Option[Consistency]
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      consistency match {
        case Some(consist) =>
          output match {
            case file @ File(_, _) =>
              convertUsingBasicSink(temporaryPath, tempoBasePath, file, storage, consist.toSaveMode)
            case cassandra @ Cassandra(_, _) =>
              CassandraSink.insertRows(
                File(JSON, temporaryPath),
                cassandra,
                tempoBasePath,
                consist.toSaveMode
              )
            case _ =>
              Left(
                DataHighwayErrorResponse(
                  "ShouldUseIntermediateSaveMode",
                  "'save-mode' field should be not present",
                  ""
                )
              )
          }
        case None =>
          output match {
            case elasticsearch @ Elasticsearch(_, _, _) =>
              ElasticSink
                .insertDocuments(
                  File(JSON, temporaryPath),
                  elasticsearch,
                  tempoBasePath,
                  storage
                )
            case Kafka(_, _) =>
              new RuntimeException("Already taken care of by kafka-to-kafka routes")
            case _ =>
              Left(
                DataHighwayErrorResponse(
                  "MissingSaveMode",
                  "Missing 'save-mode' field",
                  ""
                )
              )
          }
      }
    }
  }

  /**
    * Converts the temporary json data to the output dataset
    *
    * @param temporaryPath The temporary json path
    * @param tempoBasePath The base of the temporary json path
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @return DataHighwayResponse, otherwise a DataHighwayErrorResponse
    */
  def convertUsingBasicSink(
      temporaryPath: String,
      tempoBasePath: String,
      output: File,
      storage: Storage,
      saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwayResponse] = {
    val tempInputPath = storage match {
      case Local => new java.io.File(temporaryPath).getParent
      case HDFS  => HdfsUtils.getPathWithoutUriPrefix(new java.io.File(temporaryPath).getParent)
    }
    BasicSink.handleChannel(
      File(JSON, tempInputPath),
      output,
      Some(storage),
      Some(Consistency.toConsistency(saveMode))
    )
  }
}
