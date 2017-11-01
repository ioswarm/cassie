name := """cassie"""

organization := "de.ioswarm"

version := "0.3.0"

scalaVersion := "2.12.3"

scalacOptions ++= Seq(
//  "-feature",
  "-language:_",
  "-unchecked",
  "-deprecation",
  "-encoding", "utf8"
)

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
  "org.scala-lang" % "scala-reflect" % "2.12.3",
  "com.typesafe" % "config" % "1.3.1",
  "org.slf4j" % "slf4j-api" % "1.7.25",

  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "org.cassandraunit" % "cassandra-unit" % "3.3.0.2" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.25" % "test"
)

lazy val cassieProject = (project in file("."))
    .enablePlugins(spray.boilerplate.BoilerplatePlugin)
