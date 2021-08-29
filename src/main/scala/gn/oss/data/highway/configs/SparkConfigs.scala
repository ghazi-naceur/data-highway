package gn.oss.data.highway.configs

import gn.oss.data.highway.models.LogLevel

case class SparkConfigs(appName: String, masterUrl: String, logLevel: LogLevel)
