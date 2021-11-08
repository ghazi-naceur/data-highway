package gn.oss.data.highway.engine.extractors

import java.time.Duration
import java.util.UUID
import org.apache.kafka.streams.KafkaStreams
import org.apache.spark.sql.{SaveMode, SparkSession}
import monix.execution.Scheduler.{global => scheduler}
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.configs.HdfsUtils
import gn.oss.data.highway.engine.sinks.{BasicSink, CassandraSink, ElasticSink, PostgresSink}
import gn.oss.data.highway.models.DataHighwayRuntimeException.{
  KafkaConsumerMissingModeError,
  KafkaConsumerSupportModeError,
  MustHaveSaveModeError,
  MustNotHaveSaveModeError
}
import gn.oss.data.highway.models.{
  Cassandra,
  Consistency,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
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
  Postgres,
  PureKafkaConsumer,
  PureKafkaStreamsConsumer,
  SparkKafkaPluginConsumer,
  Storage,
  TemporaryLocation
}
import gn.oss.data.highway.utils.DataFrameUtils.sparkSession
import gn.oss.data.highway.utils._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{struct, to_json}

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object KafkaExtractor extends HdfsUtils with LazyLogging {

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
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val temporaryLocation = SharedUtils.setTempoFilePath("kafka-extractor", storage)
    val consumer = input.kafkaMode.asInstanceOf[Option[KafkaConsumer]]
    val fileSystem = SharedUtils.setFileSystem(output, storage)
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
                  output,
                  temporaryLocation,
                  fileSystem,
                  consistency,
                  brokers,
                  offset,
                  consGroup,
                  fs
                )
              })
            SharedUtils.constructIOResponse(input, output, result)
          case PureKafkaStreamsConsumer(brokers, streamAppId, offset) =>
            // Continuous job - to be triggered once
            val result = sinkWithPureKafkaStreamsConsumer(
              input.topic,
              output,
              temporaryLocation,
              fileSystem,
              consistency,
              brokers,
              offset,
              streamAppId,
              fs
            )
            SharedUtils.constructIOResponse(input, output, result)
          case SparkKafkaPluginConsumer(brokers, offset) =>
            // Batch/One-shot job - to be triggered everytime
            val result = Either.catchNonFatal {
              sinkWithSparkKafkaConnector(
                sparkSession,
                input,
                output,
                temporaryLocation,
                fileSystem,
                consistency,
                brokers,
                offset
              )
            }
            SharedUtils.constructIOResponse(input, output, result)
          case _ => Left(KafkaConsumerSupportModeError)
        }
      case None => Left(KafkaConsumerMissingModeError)
    }
  }

  /**
    * Sinks topic content in output entity using a [[PureKafkaConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param output The output File entity
    * @param tempoLocation The temporary output location
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
    output: Output,
    tempoLocation: TemporaryLocation,
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
          logger.info(s"Topic: ${data.topic()}, Key: ${data.key()}, Value: ${data.value()}, Offset: ${data
            .offset()}, Partition: ${data.partition()}")
          val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
          storage match {
            case Local =>
              FilesUtils.createFile(tempoLocation.path, s"simple-consumer-$uuid.${JSON.extension}", data.value())
            case HDFS =>
              HdfsUtils.save(fs, s"${tempoLocation.path}/simple-consumer-$uuid.${JSON.extension}", data.value())
          }
          logger.info(
            s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
              s"'${tempoLocation.path}/simple-consumer-*****${JSON.extension}'"
          )
        }
        consumed.close()
      })
    dispatchDataToOutput(tempoLocation, output, storage, consistency)
  }

  /**
    * Sinks topic content in output entity using a [[PureKafkaStreamsConsumer]]
    *
    * @param inputTopic The input kafka topic
    * @param output The output File entity
    * @param tempoLocation The temporary folder for intermediate processing
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
    output: Output,
    tempoLocation: TemporaryLocation,
    storage: Storage,
    consistency: Option[Consistency],
    brokerUrls: String,
    offset: Offset,
    streamAppId: String,
    fs: FileSystem
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      val kafkaStreamEntity = KafkaTopicConsumer.consumeWithStream(streamAppId, inputTopic, offset, brokerUrls)
      kafkaStreamEntity.dataKStream.mapValues(data => {
        val uuid: String = s"${UUID.randomUUID()}-${System.currentTimeMillis()}"
        storage match {
          case Local => FilesUtils.createFile(tempoLocation.path, s"kafka-streams-$uuid.${JSON.extension}", data)
          case HDFS  => HdfsUtils.save(fs, s"${tempoLocation.path}/kafka-streams-$uuid.${JSON.extension}", data)
        }
        logger.info(
          s"Successfully sinking '${JSON.extension}' data provided by the input topic '$inputTopic' in the output folder pattern " +
            s"'${tempoLocation.path}/kafka-streams-*****${JSON.extension}'"
        )
        dispatchDataToOutput(tempoLocation, output, storage, consistency)
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
    * @param tempoLocation The temporary folder for intermediate processing
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
    tempoLocation: TemporaryLocation,
    storage: Storage,
    consistency: Option[Consistency],
    brokerUrls: String,
    offset: Offset
  ): Either[Throwable, Unit] = {
    import session.implicits._
    val computedOffset = offset match {
      case Latest =>
        logger.warn(s"Starting offset can't be '$offset' for batch queries on Kafka. So, we'll set it at 'Earliest'.")
        Earliest
      case Earliest => offset
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
        val jsonFileName = s"spark-kafka-plugin-${UUID.randomUUID()}-${System.currentTimeMillis()}.${JSON.extension}"
        storage match {
          case Local => FilesUtils.createFile(tempoLocation.path, jsonFileName, line)
          case HDFS  => HdfsUtils.save(fs, s"${tempoLocation.path}/$jsonFileName", line)
        }
      })
    dispatchDataToOutput(tempoLocation, output, storage, consistency)
  }

  /**
    * Dispatches intermediate data to the provided output
    *
    * @param tempoLocation The temporary location for intermediate processing
    * @param output The provided output entity
    * @param storage The file system storage
    * @param consistency The Spark save mode
    * @return Unit, otherwise a Throwable
    */
  private def dispatchDataToOutput(
    tempoLocation: TemporaryLocation,
    output: Output,
    storage: Storage,
    consistency: Option[Consistency]
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      consistency match {
        case Some(consist) =>
          output match {
            case file @ File(_, _) => convertUsingBasicSink(tempoLocation, file, storage, consist.toSaveMode)
            case cassandra @ Cassandra(_, _) =>
              CassandraSink
                .insertRows(File(JSON, tempoLocation.path), cassandra, tempoLocation.basePath, consist.toSaveMode)
            case postgres @ Postgres(_, _) =>
              PostgresSink
                .insertRows(File(JSON, tempoLocation.path), postgres, tempoLocation.basePath, consist.toSaveMode)
            case _ => Left(MustNotHaveSaveModeError)
          }
        case None =>
          output match {
            case elasticsearch @ Elasticsearch(_, _, _) =>
              ElasticSink.insertDocuments(File(JSON, tempoLocation.path), elasticsearch, tempoLocation.basePath, storage)
            case _ => Left(MustHaveSaveModeError)
          }
      }
    }
  }

  /**
    * Converts the temporary json data to the output dataset
    *
    * @param tempoLocation The temporary location for intermediate processing
    * @param output The output File entity
    * @param storage The output file system storage
    * @param saveMode The output save mode
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def convertUsingBasicSink(
    tempoLocation: TemporaryLocation,
    output: File,
    storage: Storage,
    saveMode: SaveMode
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val tempInputPath = storage match {
      case Local => new java.io.File(tempoLocation.path).getParent
      case HDFS  => HdfsUtils.getPathWithoutUriPrefix(new java.io.File(tempoLocation.path).getParent)
    }
    BasicSink.handleChannel(File(JSON, tempInputPath), output, Some(storage), Some(Consistency.toConsistency(saveMode)))
  }
}
