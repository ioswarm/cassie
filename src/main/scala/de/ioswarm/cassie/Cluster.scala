package de.ioswarm.cassie

import java.net.InetAddress

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{CodecRegistry, PreparedStatement, RegularStatement, ResultSet, Session, Statement, TypeCodec, UDTValue, Cluster => CasCluster}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by andreas on 20.08.17.
  */
object Cluster {
  import scala.collection.JavaConversions._

  sys addShutdownHook close()

  private var connections: Map[String, Connection] = Map.empty[String, Connection]

  private[cassie] val settings = new Settings(ConfigFactory.load())
  val codecRegistry = new CodecRegistry

  var cluster: CasCluster = buildCluster()

  private def buildCluster() = CasCluster.builder()
    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
    .addContactPoints(settings.hosts)
    .withPort(settings.port)
    .withCodecRegistry(codecRegistry)
    .build()

  def close(): Unit = {
    connections.values.foreach(_.close())
    if (!cluster.isClosed) cluster.close()
  }

  private[cassie] def removeConnection(connection: Connection): Unit = {
    if (connections.contains(connection.keyspace)) {
      connections = connections - connection.keyspace
    }
  }

  def apply(): Connection = apply("")
  def apply(keyspace: String): Connection = {
    if (cluster.isClosed) cluster = buildCluster()
    if (!connections.contains(keyspace)) {
      if (keyspace.nonEmpty) StatementBuilder.keyspace(keyspace).create()
      connections += keyspace -> Connection(keyspace)
    }
    connections(keyspace)
  }

  def connect(): Connection = apply()
  def connect(keyspace: String): Connection = apply(keyspace)

  def typeCodec(keyspace: String, name: String): TypeCodec[UDTValue] = codecRegistry.codecFor(cluster.getMetadata.getKeyspace(keyspace).getUserType(name))

  def registerType[T <: TypeCodec[_]](t: T): Unit = codecRegistry.register(t)

  def activeConnectionSize: Int = connections.count(!_._2.isClosed)

  def activeConnections: Seq[Connection] = connections.filter(!_._2.isClosed).values.toSeq

  sealed case class Connection(keyspace: String = "") {

    val session: Session = if (keyspace.length > 0) Cluster.cluster.connect(keyspace) else Cluster.cluster.connect()

    def isClosed: Boolean = session.isClosed

    def close(): Unit = {
      if (!session.isClosed) session.close()
      Cluster.removeConnection(this)
    }

    @throws[Exception]
    def prepare(query: String): PreparedStatement = session.prepare(query)
    @throws[Exception]
    def prepare(statement: RegularStatement): PreparedStatement = session.prepare(statement)

    def prepareAsync(query: String)(implicit ex: ExecutionContext): Future[PreparedStatement] = Future{
      prepare(query)
    }(ex)
    def prepareAsync(statement: RegularStatement)(implicit ex: ExecutionContext): Future[PreparedStatement] = Future{
      prepare(statement)
    }(ex)

    @throws[Exception]
    def execute(query: String): ResultSet = session.execute(query)
    @throws[Exception]
    def execute(statement: Statement): ResultSet = session.execute(statement)

    def executeAsync(query: String)(implicit ex: ExecutionContext): Future[ResultSet] = Future {
      execute(query)
    }(ex)
    def executeAsync(statement: Statement)(implicit ex: ExecutionContext): Future[ResultSet] = Future {
      execute(statement)
    }(ex)

  }

  private[cassie] class Settings(config: Config) {

    def hosts: List[InetAddress] = if (config.hasPath("cassie.hosts"))
        config.getStringList("cassie.hosts").map(host => InetAddress.getByName(host)).toList
      else
        List(InetAddress.getLocalHost)

    def port: Int = if (config.hasPath("cassie.port"))
      config.getInt("cassie.port")
    else 9042

  }
}
