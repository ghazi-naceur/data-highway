package io.oss.data.highway.rest

import cats.data.Kleisli
import cats.effect._
import io.circe.Json
import org.http4s.implicits._
import io.circe.syntax._
import io.oss.data.highway.Main
import io.oss.data.highway.configuration.SparkConfigs
import io.oss.data.highway.model.Route
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe._
import org.http4s.dsl.io._
import pureconfig.ConfigSource

object ConversionController {
  import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage
  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case GET -> Root / "conversion" =>
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "route" =>
        for {
          ioJson <- req.asJson
          decodedRoute = parseRouteBody(ioJson)
          decodedConf = parseSparkConfBody(ioJson)
          _ = Main.apply(decodedConf, decodedRoute)
          resp <- Ok(200)
        } yield resp
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
