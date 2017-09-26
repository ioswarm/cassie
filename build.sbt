import spray.boilerplate.BoilerplatePlugin._

name := """cassie"""

organization := "de.ioswarm"

version := "0.2.0"

scalaVersion := "2.11.11"

scalacOptions ++= Seq(
//  "-feature",
  "-language:_",
  "-unchecked",
  "-deprecation",
  "-encoding", "utf8"
)

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0",
  "org.scala-lang" % "scala-reflect" % "2.11.11",
  "com.typesafe" % "config" % "1.3.0",
  "org.slf4j" % "slf4j-api" % "1.7.12",

  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.5" % "test",
  "org.cassandraunit" % "cassandra-unit" % "3.0.0.1" % "test",
  "org.slf4j" % "slf4j-simple" % "1.7.12"
)

lazy val cassieProject = (project in file("."))
    .enablePlugins(spray.boilerplate.BoilerplatePlugin)
