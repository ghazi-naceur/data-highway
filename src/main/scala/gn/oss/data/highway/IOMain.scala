package gn.oss.data.highway

import cats.effect._
import gn.oss.data.highway.controllers.ConversionController
import gn.oss.data.highway.utils.SharedUtils.getBanner
import org.http4s.server.blaze._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object IOMain extends IOApp {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  override implicit val timer: Timer[IO] = IO.timer(global)

  override def run(args: List[String]): IO[ExitCode] = {
    BlazeServerBuilder[IO](global)
      .withBanner(getBanner)
      .bindHttp(5555, "localhost")
      .withHttpApp(ConversionController.httpRequests)
      .withIdleTimeout(Duration.Inf)
      .withResponseHeaderTimeout(Duration.Inf)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }
}
