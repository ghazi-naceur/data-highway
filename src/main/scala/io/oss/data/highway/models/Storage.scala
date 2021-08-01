package io.oss.data.highway.models

sealed trait Storage

case object Local extends Storage
case object HDFS  extends Storage
