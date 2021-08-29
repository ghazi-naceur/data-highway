package io.oss.data.highway.controllers

import cats.data.Kleisli
import cats.effect._
import org.http4s.implicits._
import io.circe.syntax._
import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.engine.Dispatcher
import io.oss.data.highway.models.{Query, Route}
import org.apache.log4j.Logger
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe._
import org.http4s.dsl.io._

object ConversionController {

  val logger: Logger = Logger.getLogger(ConversionController.getClass.getName)

  import pureconfig.generic.auto._ // To be kept, even though intellij didn't recognize its usage
  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case req @ GET -> Root / "conversion" =>
        logger.info("GET Request received : " + req.toString())
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "query" =>
        logger.info("POST Request received : " + req.toString())
        val ioResponse = req.asJson.map(request => {
          val decodedOps =
            ConfigLoader().loadConfigsFromString[Query]("query", request.asJson.toString())
          Dispatcher.apply(decodedOps)
        })
        ioResponse.flatMap {
          case Right(_)        => Ok(200)
          case Left(exception) => throw new RuntimeException(exception)
        }
      case req @ POST -> Root / "conversion" / "route" =>
        logger.info("POST Request received : " + req.toString())
        val ioResponse = req.asJson.map(request => {
          val decodedRoute =
            ConfigLoader().loadConfigsFromString[Route]("route", request.asJson.toString())
          Dispatcher.apply(decodedRoute)
        })
        ioResponse.flatMap {
          case Right(_)        => Ok(200)
          case Left(exception) => throw new RuntimeException(exception)
        }
    }
    .orNotFound
}
