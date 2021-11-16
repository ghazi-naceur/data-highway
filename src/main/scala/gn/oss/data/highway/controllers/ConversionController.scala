package gn.oss.data.highway.controllers

import cats.data.Kleisli
import cats.effect._
import com.typesafe.scalalogging.LazyLogging
import gn.oss.data.highway.configs.ConfigLoader
import gn.oss.data.highway.engine.Dispatcher
import gn.oss.data.highway.models
import gn.oss.data.highway.models.Route
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object ConversionController extends LazyLogging {

  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case req @ GET -> Root / "conversion" =>
        logger.info("GET Request received : " + req.toString())
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "query" => handleRestQuery("query", req)
      case req @ POST -> Root / "conversion" / "route" => handleRestQuery("route", req)
    }
    .orNotFound

  /**
    * Handles the input HTTP query
    *
    * @param param The HTTP query body element name
    * @param request The input HTTP query
    * @return IO Response
    */
  private def handleRestQuery(param: String, request: Request[IO]): IO[Response[IO]] = {
    import pureconfig.generic.auto._
    logger.info("POST Request received : " + request.toString())
    val ioResponse = request.asJson.map(request => {
      val parsedRestQuery = param match {
        case "route" => ConfigLoader().loadConfigsFromString[Route](param, request.asJson.toString())
        case "query" => ConfigLoader().loadConfigsFromString[models.Query](param, request.asJson.toString())
      }
      Dispatcher.apply(parsedRestQuery)
    })
    ioResponse.flatMap {
      case Right(dhr) => Ok(dhr)
      case Left(dhe)  => InternalServerError(dhe)
    }
  }
}
