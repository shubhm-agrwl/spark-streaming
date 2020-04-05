import sbt.project

name := "spark-streaming"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2" excludeAll (excludeJpountz)

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2" excludeAll (excludeJpountz)

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" excludeAll (excludeJpountz)

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.2" excludeAll (excludeJpountz)

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.2.1" excludeAll (excludeJpountz)

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"

libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.2.0"

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"

libraryDependencies += "org.apache.ignite" % "ignite-core" % "2.4.0"

libraryDependencies += "org.apache.ignite" % "ignite-spring" % "2.4.0"

// libraryDependencies += "com.eed3si9n" % "sbt-assembly_2.11" % "0.14.5"

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "0.10.2.1" excludeAll (excludeJpountz)

libraryDependencies += kafkaClients

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}