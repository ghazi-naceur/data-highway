package gn.oss.data.highway.configs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import pureconfig.generic.auto._

case class HadoopConfigs(host: String, port: String)

trait HdfsUtils {
  val hadoopConf: HadoopConfigs = ConfigLoader().loadConfigs[HadoopConfigs]("hadoop")
  val conf: Configuration       = new Configuration()
  conf.set("fs.defaultFS", s"${hadoopConf.host}:${hadoopConf.port}")
  val fs: FileSystem = FileSystem.get(conf)
}
