lazy val settings = Seq(
  organization := "de.ioswarm"
  , version := "0.4.0"
  , scalaVersion := "2.12.3"
  , scalacOptions ++= Seq(
    "-feature"
    , "-language:_"
    , "-unchecked"
    , "-deprecation"
    , "-encoding", "utf8"
    , "target:jvm-1.8"
  )
)

lazy val root = project.in(file("."))
  .settings(settings)
  .settings(
    name := "cassie-project"
  )
  .aggregate(
    cassie
    , cassieAkkaStream
  )

lazy val cassie = project.in(file("cassie"))
  .settings(settings)
  .settings(
    name := "cassie"
    , libraryDependencies ++= Seq(
      lib.cassandraDriver
      , lib.scalaReflect
      , lib.typesafeConfig
      , lib.slf4jApi

      , lib.scalaTest
      , lib.scalaCheck
      , lib.cassandraUnit
      , lib.slf4jTest
    )
  )
  .enablePlugins(spray.boilerplate.BoilerplatePlugin)

lazy val cassieAkkaStream = project.in(file("cassie-akka-stream"))
  .settings(settings)
  .settings(
    name := "cassie-akka-stream"
    , libraryDependencies ++= Seq(
      lib.akkaStream
    )
  )
  .dependsOn(
    cassie
  )


lazy val lib = new {
  object Version {
    val scala = "2.12.3"
    val cassandraDriver = "3.3.0"
    val akka = "2.5.4"
    val typesafeConfig = "1.3.1"
    val slf4j = "1.7.15"

    val scalaTest = "3.0.4"
    val scalaCheck = "1.13.5"
    val cassandraUnit = "3.3.0.2"
  }

  val scalaReflect = "org.scala-lang" % "scala-reflect" % Version.scala
  val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % Version.cassandraDriver
  val typesafeConfig = "com.typesafe" % "config" % Version.typesafeConfig
  val slf4jApi = "org.slf4j" % "slf4j-api" % Version.slf4j

  val akkaStream = "com.typesafe.akka" %% "akka-stream" % Version.akka

  val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck % "test"
  val cassandraUnit = "org.cassandraunit" % "cassandra-unit" % Version.cassandraUnit % "test"
  val slf4jTest = "org.slf4j" % "slf4j-simple" % Version.slf4j % "test"
}