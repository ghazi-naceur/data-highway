package gn.oss.data.highway.engine.sinks

import gn.oss.data.highway.configs.{AppUtils, HdfsUtils}
import gn.oss.data.highway.models
import gn.oss.data.highway.utils._
import org.apache.hadoop.fs.FileSystem
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import java.io.File
import java.time.Duration
import java.util.{Properties, UUID}
import scala.sys.ShutdownHookThread
import cats.implicits._
import gn.oss.data.highway.models.DataHighwayRuntimeException.{
  KafkaMirrorSupportModeError,
  KafkaProducerSupportModeError,
  MustHaveFileSystemError,
  MustHaveSaveModeError
}
import gn.oss.data.highway.models.{
  DataHighwayError,
  DataHighwayErrorResponse,
  DataHighwaySuccessResponse,
  DataType,
  HDFS,
  Kafka,
  Local,
  Offset,
  PureKafkaProducer,
  PureKafkaStreamsProducer,
  SparkKafkaPluginProducer,
  SparkKafkaPluginStreamsProducer,
  Storage,
  XLSX
}

object KafkaSink extends HdfsUtils with AppUtils {

  val logger: Logger = Logger.getLogger(KafkaSink.getClass.getName)
  // todo can be replaced by SharedUtils.setTempoFilePath()
  val generated: String        = s"${UUID.randomUUID()}-${System.currentTimeMillis().toString}"
  val checkpointFolder: String = s"${appConf.tmpWorkDir}/checkpoint-$generated"

  /**
    * Handles Kafka sink
    *
    * @param input The input File entity
    * @param output The output Kafka entity
    * @param storage The file system storage
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def handleKafkaChannel(
      input: models.File,
      output: Kafka,
      storage: Option[Storage]
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    val basePath = new File(input.path).getParent
    storage match {
      case Some(value) =>
        value match {
          case HDFS =>
            val result = for {
              list <-
                HdfsUtils
                  .listFolders(fs, input.path)
                  .traverse(folders => {
                    folders.traverse(folder =>
                      publishFilesContentToTopic(input.dataType, folder, output, basePath, storage)
                    )
                  })
                  .flatten
              _ = HdfsUtils.cleanup(fs, input.path)
            } yield list
            SharedUtils.constructIOResponse(input, output, result)
          case Local =>
            val result = for {
              folders <- FilesUtils.listNonEmptyFoldersRecursively(input.path)
              list <-
                folders
                  .traverse(folder =>
                    publishFilesContentToTopic(input.dataType, folder, output, basePath, storage)
                  )
              _ = FilesUtils.cleanup(input.path)
            } yield list
            SharedUtils.constructIOResponse(input, output, result)
        }
      case None => Left(MustHaveFileSystemError)
    }
  }

  /**
    * Publishes files' content to Kafka topic
    *
    * @param input The input folder that contains data to be send
    * @param output The output Kafka entity
    * @param basePath The base path of input path
    * @param storage The input file system storage : Local or HDFS
    * @return Any, otherwise a Throwable
    */
  def publishFilesContentToTopic(
      inputDataType: DataType,
      input: String,
      output: Kafka,
      basePath: String,
      storage: Option[Storage]
  ): Either[Throwable, Any] = {
    (storage, output.kafkaMode) match {
      case (Some(filesystem), Some(km)) =>
        KafkaUtils
          .verifyTopicExistence(output.topic, km.brokers, enableTopicCreation = true)
        km match {
          case PureKafkaProducer(brokers) =>
            Either.catchNonFatal {
              publishPathContent(
                inputDataType,
                input,
                output.topic,
                basePath,
                filesystem,
                brokers,
                fs
              )
            }
          case SparkKafkaPluginProducer(brokers) =>
            publishWithSparkKafkaPlugin(
              inputDataType,
              input,
              filesystem,
              brokers,
              output.topic,
              basePath,
              fs
            )
          case _ => Left(KafkaProducerSupportModeError)
        }
      case _ => Left(MustHaveFileSystemError)
    }
  }

  /**
    * Mirrors kafka topic
    *
    * @param input The input Kafka entity
    * @param output The output Kafka entity
    * @return DataHighwaySuccessResponse, otherwise a DataHighwayErrorResponse
    */
  def mirrorTopic(
      input: Kafka,
      output: Kafka
  ): Either[DataHighwayErrorResponse, DataHighwaySuccessResponse] = {
    input.kafkaMode match {
      case Some(km) =>
        val props = new Properties()
        props.put("bootstrap.servers", km.brokers)
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val prod = new KafkaProducer[String, String](props)
        KafkaUtils
          .verifyTopicExistence(output.topic, km.brokers, enableTopicCreation = true)
        km match {
          case PureKafkaStreamsProducer(brokers, streamAppId, offset) =>
            val result = runStream(streamAppId, input.topic, brokers, output.topic, offset)
            SharedUtils.constructIOResponse(input, output, result)
          case SparkKafkaPluginStreamsProducer(brokers, offset) =>
            val result = Either.catchNonFatal {
              val thread = new Thread(() => {
                publishWithSparkKafkaStreamsPlugin(
                  input.topic,
                  prod,
                  brokers,
                  output.topic,
                  checkpointFolder,
                  offset
                )
              })
              thread.start()
            }
            SharedUtils.constructIOResponse(input, output, result)
          case _ => Left(KafkaMirrorSupportModeError)
        }
      case None => Left(MustHaveSaveModeError)
    }
  }

  /**
    * Publishes message via [[SparkKafkaPluginProducer]]
    *
    * @param inputPath The path that contains json data to be send
    * @param storage The input file system storage : Local or HDFS
    * @param brokers The kafka brokers urls
    * @param outputTopic The output topic
    * @param fs The provided File System
    * @return Unit, otherwise an Error
    */
  private[engine] def publishWithSparkKafkaPlugin(
      inputDataType: DataType,
      inputPath: String,
      storage: Storage,
      brokers: String,
      outputTopic: String,
      basePath: String,
      fs: FileSystem
  ): Either[Throwable, List[String]] = {
    logger.info(s"Sending data through Spark Kafka Plugin to '$outputTopic'.")
    inputDataType match {
      case XLSX =>
        storage match {
          case HDFS =>
            HdfsUtils
              .listFilesRecursively(fs, inputPath)
              .traverse(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file)
                  .map(df => publishToTopicWithConnector(brokers, outputTopic, df))
              })
          case Local =>
            FilesUtils
              .listFilesRecursively(new File(inputPath), inputDataType.extension)
              .toList
              .traverse(file => {
                DataFrameUtils
                  .loadDataFrame(inputDataType, file.getAbsolutePath)
                  .map(df => publishToTopicWithConnector(brokers, outputTopic, df))
              })
        }
        storage match {
          case HDFS  => HdfsUtils.movePathContent(fs, inputPath, basePath)
          case Local => FilesUtils.movePathContent(inputPath, s"$basePath/processed")
        }
      case _ =>
        storage match {
          case HDFS =>
            HdfsUtils
              .listFolders(fs, inputPath)
              .flatMap(paths => HdfsUtils.filterNonEmptyFolders(fs, paths))
              .map(paths => {
                paths
                  .flatTraverse(path => {
                    DataFrameUtils
                      .loadDataFrame(inputDataType, path)
                      .map(df => publishToTopicWithConnector(brokers, outputTopic, df))
                    HdfsUtils.movePathContent(fs, path, basePath)
                  })
              })
              .flatten

          case Local =>
            FilesUtils
              .listNonEmptyFoldersRecursively(inputPath)
              .map(paths => {
                paths
                  .flatTraverse(path => {
                    DataFrameUtils
                      .loadDataFrame(inputDataType, path)
                      .map(df => publishToTopicWithConnector(brokers, outputTopic, df))
                    FilesUtils
                      .movePathContent(new File(path).getAbsolutePath, s"$basePath/processed")
                  })
              })
              .flatten
        }
    }
  }

  /**
    * Publishes dataframe to topic using the Spark Kafka Connector
    * @param brokers The Kafka brokers
    * @param outputTopic THe output Kafka Topic
    * @param dataframe The Dataframe to be published
    */
  private def publishToTopicWithConnector(
      brokers: String,
      outputTopic: String,
      dataframe: DataFrame
  ): Unit = {
    // todo check exception
    import org.apache.spark.sql.functions.{struct, to_json}
    dataframe
      .select(to_json(struct("*")).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", outputTopic)
      .save()
  }

  /**
    * Publishes a message via Spark [[SparkKafkaPluginStreamsProducer]]
    *
    * @param inputTopic The input Kafka topic
    * @param producer The Kafka Producer
    * @param bootstrapServers The kafka brokers urls
    * @param outputTopic The output Kafka topic
    * @param checkpointFolder The checkpoint folder
    * @param offset The Kafka offset from where the message consumption will begin
    * @return Unit, otherwise an Error
    */
  private def publishWithSparkKafkaStreamsPlugin(
      inputTopic: String,
      producer: KafkaProducer[String, String],
      bootstrapServers: String,
      outputTopic: String,
      checkpointFolder: String,
      offset: Offset
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      DataFrameUtils.sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", offset.value)
        .option("subscribe", inputTopic)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .writeStream
        .format("kafka") // console
        .option("truncate", false)
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("checkpointLocation", checkpointFolder)
        .option("topic", outputTopic)
        .start()
        .awaitTermination()
    }
  }

  /**
    * Runs Kafka stream via [[PureKafkaStreamsProducer]]
    *
    * @param streamAppId The Kafka stream application id
    * @param inputTopic The Kafka input topic
    * @param bootstrapServers The kafka brokers urls
    * @param outputTopic The Kafka output topic
    * @param offset The Kafka offset from where the message consumption will begin
    * @return ShutdownHookThread, otherwise a DataHighwayError
    */
  private[engine] def runStream(
      streamAppId: String,
      inputTopic: String,
      bootstrapServers: String,
      outputTopic: String,
      offset: Offset
  ): Either[Throwable, ShutdownHookThread] = {
    Either.catchNonFatal {
      import org.apache.kafka.streams.scala.ImplicitConversions._
      import org.apache.kafka.streams.scala.Serdes._
      val props = new Properties
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, streamAppId)
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset.value)

      val builder     = new StreamsBuilder
      val dataKStream = builder.stream[String, String](inputTopic)
      dataKStream.to(outputTopic)
      val streams = new KafkaStreams(builder.build(), props)
      logger.info(s"Sending data through Kafka streams from '$inputTopic' to '$outputTopic'.")
      streams.start()

      sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
      }
    }.leftMap(thr => DataHighwayError(thr.getMessage, thr.getCause.toString))
  }

  /**
    * Publishes json files located under a provided path to Kafka topic.
    *
    * @param inputPath The input folder
    * @param topic The output Kafka topic
    * @param storage The input file system storage : Local or HDFS
    * @param brokers The Kafka brokers
    * @param fs The provided File System
    * @return Any, otherwise an Error
    */
  private[engine] def publishPathContent(
      inputDataType: DataType,
      inputPath: String,
      topic: String,
      basePath: String,
      storage: Storage,
      brokers: String,
      fs: FileSystem
  ): Either[Throwable, Any] = {
    Either.catchNonFatal {
      val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      val producer = new KafkaProducer[String, String](props)
      inputDataType match {
        case XLSX =>
          val content = storage match {
            case HDFS =>
              HdfsUtils
                .listFilesRecursively(fs, inputPath)
                .flatTraverse(file =>
                  DataFrameUtils
                    .loadDataFrame(inputDataType, file)
                    .flatMap(df => DataFrameUtils.convertDataFrameToJsonLines(df))
                )
            case Local =>
              FilesUtils
                .listFilesRecursively(new File(inputPath), inputDataType.extension)
                .toList
                .flatTraverse(file =>
                  DataFrameUtils
                    .loadDataFrame(inputDataType, file.getAbsolutePath)
                    .flatMap(df => DataFrameUtils.convertDataFrameToJsonLines(df))
                )
          }
          content.map(lines => publishFileContent(lines, topic, producer))
        case _ =>
          DataFrameUtils
            .loadDataFrame(inputDataType, inputPath)
            .traverse(df => DataFrameUtils.convertDataFrameToJsonLines(df))
            .map(_.map(lines => publishFileContent(lines, topic, producer)))
      }
      storage match {
        case HDFS  => HdfsUtils.movePathContent(fs, inputPath, basePath)
        case Local => FilesUtils.movePathContent(inputPath, s"$basePath/processed")
      }
    }
  }

  /**
    * Publishes the content of the json file via [[PureKafkaProducer]]
    *
    * @param content The json content as a list of json lines
    * @param topic The destination topic
    * @param producer The Kafka producer
    * @return Unit, otherwise a Throwable
    */
  private[engine] def publishFileContent(
      content: List[String],
      topic: String,
      producer: KafkaProducer[String, String]
  ): Either[Throwable, Unit] = {
    Either.catchNonFatal {
      content
        .foreach(line => {
          val uuid = UUID.randomUUID().toString
          val data = new ProducerRecord[String, String](topic, uuid, line)
          producer.send(data)
          logger.info(s"Topic: '$topic' - Sent data: '$line'")
        })
    }
  }
}
