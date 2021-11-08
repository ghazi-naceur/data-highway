package gn.oss.data.highway.utils

import gn.oss.data.highway.build.info.BuildInfo

object Constants {
  val EMPTY = ""
  private val BANNER_STRINGS = List(
    ",------.              ,--.              ,--.  ,--. ,--.         ,--.",
    "|  .-.  \\   ,--,--. ,-'  '-.  ,--,--.   |  '--'  | `--'  ,---.  |  ,---.  ,--.   ,--.  ,--,--. ,--. ,--.",
    "|  |  \\  : ' ,-.  | '-.  .-' ' ,-.  |   |  .--.  | ,--. | .-. | |  .-.  | |  |.'.|  | ' ,-.  |  \\  '  /",
    "|  '--'  / \\ '-'  |   |  |   \\ '-'  |   |  |  |  | |  | ' '-' ' |  | |  | |   .'.   | \\ '-'  |   \\   '",
    "`-------'   `--`--'   `--'    `--`--'   `--'  `--' `--' .`-  /  `--' `--' '--'   '--'  `--`--' .-'  /",
    "                                                        `---'                                  `---' "
  )
  val banner: List[String] = {
    val head = BANNER_STRINGS.dropRight(1)
    val lastElement = (BANNER_STRINGS.diff(head) ::: head.diff(BANNER_STRINGS)).head + s" version ${BuildInfo.version}"
    head :+ lastElement
  }
}
