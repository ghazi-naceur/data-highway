package io.oss.data.highway.configuration

import io.oss.data.highway.model.LogLevel

case class SparkConfig(appName: String, masterUrl: String, logLevel: LogLevel)
