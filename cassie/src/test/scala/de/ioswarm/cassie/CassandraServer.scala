package de.ioswarm.cassie

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.duration._

/**
  * Created by andreas on 14.07.16.
  */
object CassandraServer {

  def startEmbeddedServer(timeout: FiniteDuration = 10.seconds) =
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-server-config.yaml", timeout.toMillis)

  def clean() = EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()

}

trait CassandraStartStop extends BeforeAndAfterAll {
  this: Suite =>

  override protected def beforeAll(): Unit = {
    CassandraServer.startEmbeddedServer(30.seconds)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    CassandraServer.clean()
    super.afterAll()
  }

}