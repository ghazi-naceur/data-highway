package io.oss.data.highway.rest

import cats.data.Kleisli
import cats.effect._
import org.http4s.implicits._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.oss.data.highway.model.Route
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe._
import org.http4s.dsl.io._

object ConversionController {

  val httpRequests: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes
    .of[IO] {
      case GET -> Root / "conversion" =>
        Ok(s"Data Highway REST API.")
      case req @ POST -> Root / "conversion" / "route" =>
        for {
          ioJson <- req.asJson
          decoded = decode[Route](ioJson.asJson.toString()) match {
            case Right(value) => value.asInstanceOf[Route]
            case Left(ex) =>
              throw new RuntimeException(s"This request is incorrect due '$ex'")
          }
          _ = println(decoded)
          resp <- Ok(decoded)
        } yield resp
    }
    .orNotFound
}
