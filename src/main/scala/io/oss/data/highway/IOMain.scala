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
//      .withIdleTimeout(Duration.Inf)
//      .withResponseHeaderTimeout(Duration.Inf)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }

  def getBanner: List[String] = {
    val lines = Source
      .fromResource("banner.txt")
      .getLines()
      .toList
    val head = lines.dropRight(1)
    val lastElement = (lines.diff(head) ::: head.diff(lines)).head + s" version ${BuildInfo.version}"
    head :+ lastElement
  }
}
