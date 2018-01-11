lazy val settings = Seq(
  name := "cassie"
  , organization := "de.ioswarm"
  , version := "0.3.1"
  , scalaVersion := "2.12.4"
  , scalacOptions ++= Seq(
    "-language:_"
    , "-unchecked"
    , "-deprecation"
    , "-encoding", "UTF-8"
  )
)

lazy val root = project.in(file("."))
.settings(settings)
.settings(
  name := "cassie-project"
)
.aggregate(
  core
  , akka
)

lazy val core = project.in(file("core"))
.settings(settings)
.settings(
  name := "cassie"
  , libraryDependencies ++= Seq(
    lib.cassandra
    , lib.scalaReflect
    , lib.config
    , lib.slf4jAPI

    , lib.scalaTest
    , lib.scalaCheck
    , lib.cassandraUnit
    , lib.slf4jTest
  )
)
.enablePlugins(
  spray.boilerplate.BoilerplatePlugin
)

lazy val akka = project.in(file("akka-stream"))
  .settings(settings)
  .settings(
    name := "cassie-akka-stream"
    , libraryDependencies ++= Seq(
      lib.akkaStream

      , lib.scalaTest
      , lib.scalaCheck
      , lib.slf4jTest
    )
  )
  .dependsOn(
    core
  )

lazy val lib = new {
  object Version {
    val cassandra = "3.3.2"
    val scala = "2.12.4"

    val config = "1.3.2"

    val slf4j = "1.7.7"

    val scalaTest = "3.0.4"
    val scalaCheck = "1.13.5"

    val cassandraUnt = "3.3.0.2"

    val akka = "2.5.8"
  }

  val cassandra = "com.datastax.cassandra" % "cassandra-driver-core" % Version.cassandra
  val scalaReflect = "org.scala-lang" % "scala-reflect" % Version.scala
  val config = "com.typesafe" % "config" % Version.config
  val slf4jAPI = "org.slf4j" % "slf4j-api" % Version.slf4j

  val akkaStream = "com.typesafe.akka" %% "akka-stream" % Version.akka

  val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  val cassandraUnit = "org.cassandraunit" % "cassandra-unit" % Version.cassandraUnt % "test"
  val slf4jTest = "org.slf4j" % "slf4j-simple" % Version.slf4j % "test"

}
