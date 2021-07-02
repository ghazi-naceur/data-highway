package io.oss.data.highway.models

sealed trait FileSystem

case object Local extends FileSystem
case object HDFS  extends FileSystem
