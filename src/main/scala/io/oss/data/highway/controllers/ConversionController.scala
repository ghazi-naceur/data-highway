package io.oss.data.highway.controllers

import cats.data.Kleisli
import cats.effect._
import io.circe.syntax._
import io.oss.data.highway.configs.ConfigLoader
import io.oss.data.highway.engine.Dispatcher
import io.oss.data.highway.models.{Route, Query}
import org.apache.log4j.Logger
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.implicits._

object ConversionController {

  val logger: Logger = Logger.getLogger(ConversionController.getClass.getName)
  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case req @ GET -> Root / "conversion" =>
        logger.info("GET Request received : " + req.toString())
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "query" =>
        handleRestQuery("query", req)
      case req @ POST -> Root / "conversion" / "route" =>
        handleRestQuery("route", req)
    }
    .orNotFound

  private def handleRestQuery(param: String, req: Request[IO]): IO[Response[IO]] = {
    import pureconfig.generic.auto._
    logger.info("POST Request received : " + req.toString())
    val ioResponse = req.asJson.map(request => {
      val parsedRestQuery = param match {
        case "route" =>
          ConfigLoader().loadConfigsFromString[Route](param, request.asJson.toString())
        case "query" =>
          ConfigLoader().loadConfigsFromString[Query](param, request.asJson.toString())
      }
      Dispatcher.apply(parsedRestQuery)
    })
    ioResponse.flatMap {
      case Right(_)        => Ok(200)
      case Left(exception) => throw new RuntimeException(exception)
    }
  }
}
