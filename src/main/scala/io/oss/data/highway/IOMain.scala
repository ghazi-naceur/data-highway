package io.oss.data.highway

import cats.effect._
import io.oss.data.highway.build.info.BuildInfo
import org.http4s.server.blaze._
import io.oss.data.highway.rest.ConversionController

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

object IOMain extends IOApp {

  // Needed by `BlazeServerBuilder`. Provided by `IOApp`.
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  override implicit val timer: Timer[IO] = IO.timer(global)

  override def run(args: List[String]): IO[ExitCode] = {

    BlazeServerBuilder[IO](global)
      .withBanner(getBanner)
      .bindHttp(5555, "localhost")
      .withHttpApp(ConversionController.httpRequests)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }

  def getBanner: List[String] = {
    Source
      .fromResource("banner.txt")
      .getLines()
      .toList :+ s"version ${BuildInfo.version}"
  }
}
