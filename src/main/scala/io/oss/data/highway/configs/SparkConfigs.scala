package io.oss.data.highway.configs

import io.oss.data.highway.models.LogLevel

case class SparkConfigs(appName: String, masterUrl: String, logLevel: LogLevel)
