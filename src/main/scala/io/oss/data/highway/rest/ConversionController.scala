package io.oss.data.highway.rest

import cats.data.Kleisli
import cats.effect._
import io.circe.Json
import org.http4s.implicits._
import io.circe.syntax._
import io.oss.data.highway.Main
import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.Route
import org.apache.log4j.Logger
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe._
import org.http4s.dsl.io._
import pureconfig.ConfigSource

object ConversionController {

  val logger: Logger = Logger.getLogger(ConversionController.getClass.getName)

  import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage
  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case req @ GET -> Root / "conversion" =>
        logger.info("GET Request received : " + req.toString())
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "route" =>
        logger.info("POST Request received : " + req.toString())
        val ioResponse = req.asJson.map(request => {
          val decodedRoute = parseRouteBody(request)
          val decodedConf = parseSparkConfBody(request)
          Main.apply(decodedConf, decodedRoute)
        })
        ioResponse.flatMap {
          case Right(_)        => Ok(200)
          case Left(exception) => throw new RuntimeException(exception)
        }
    }
    .orNotFound

  // todo to be generalized, but there is an issue when loading Generic types
  private def parseRouteBody(ioJson: Json,
                             jsonElement: String = "route"): Route = {
    ConfigSource
      .string(ioJson.asJson.toString())
      .at(jsonElement)
      .load[Route] match {
      case Right(value) => value
      case Left(exception) =>
        throw new RuntimeException(
          s"This request is incorrect due '$exception'")
    }
  }

  private def parseSparkConfBody(
      ioJson: Json,
      jsonElement: String = "spark"): SparkConfigs = {
    ConfigSource
      .string(ioJson.asJson.toString())
      .at(jsonElement)
      .load[SparkConfigs] match {
      case Right(value) => value
      case Left(exception) =>
        throw new RuntimeException(
          s"This request is incorrect due '$exception'")
    }
  }
}
