package de.ioswarm.cassie

import java.net.InetAddress

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{CodecRegistry, PreparedStatement, RegularStatement, ResultSet, Session, Statement, TypeCodec, UDTValue, Cluster => CasCluster}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Created by andreas on 20.08.17.
  */
object Cluster {

  import scala.collection.JavaConverters._

  sys addShutdownHook close()

  private var connections: Map[Keyspace, Connection] = Map.empty[Keyspace, Connection]

  private[cassie] val settings = new Settings(ConfigFactory.load())
  val codecRegistry = new CodecRegistry

  var cluster: CasCluster = buildCluster()

  private def buildCluster() = CasCluster.builder()
    .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
    .addContactPoints(settings.hosts.asJava)
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

  def apply(): Connection = apply(Keyspace())
  def apply(keyspace: Keyspace): Connection = {
    if (cluster.isClosed) cluster = buildCluster()
    if (!connections.contains(keyspace)) {
      if (!keyspace.isRoot) keyspace.create()
      connections += keyspace -> Connection(keyspace)
    }
    connections(keyspace)
  }

  def connect(): Connection = apply()
  def connect(keyspace: Keyspace): Connection = apply(keyspace)

  def typeCodec(keyspace: String, name: String): TypeCodec[UDTValue] = codecRegistry.codecFor(cluster.getMetadata.getKeyspace(keyspace).getUserType(name))

  def registerType[T <: TypeCodec[_]](t: T): Unit = codecRegistry.register(t)

  def activeConnectionSize: Int = connections.count(!_._2.isClosed)

  def activeConnections: Seq[Connection] = connections.filter(!_._2.isClosed).values.toSeq

  sealed case class Connection(keyspace: Keyspace = Keyspace()) {

    import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

    val session: Session = if (keyspace.isRoot) Cluster.cluster.connect() else Cluster.cluster.connect(keyspace.keyspace)

    private var statements: Map[Int, PreparedStatement] = Map.empty[Int, PreparedStatement]

    def isClosed: Boolean = session.isClosed

    def close(): Unit = {
      if (!session.isClosed) session.close()
      Cluster.removeConnection(this)
    }

    def toFuture[T](fut: ListenableFuture[T]): Future[T] = {
      val p = Promise[T]
      Futures.addCallback(fut, new FutureCallback[T]{
        override def onSuccess(result: T): Unit = p.success(result)

        override def onFailure(t: Throwable): Unit = p.failure(t)
      })
      p.future
    }

    @throws[Exception]
    def prepare(query: String): PreparedStatement = {
      if (!statements.contains(query.hashCode))
        statements += query.hashCode -> session.prepare(query)
      statements(query.hashCode)
    }
    @throws[Exception]
    def prepare(statement: RegularStatement): PreparedStatement = session.prepare(statement)

    def prepareAsync(query: String): Future[PreparedStatement] = toFuture(session.prepareAsync(query))

    def prepareAsync(statement: RegularStatement): Future[PreparedStatement] = toFuture(session.prepareAsync(statement))

    @throws[Exception]
    def execute(query: String): ResultSet = session.execute(query)
    @throws[Exception]
    def execute(statement: Statement): ResultSet = session.execute(statement)

    def executeAsync(query: String): Future[ResultSet] = toFuture(session.executeAsync(query))
    def executeAsync(statement: Statement): Future[ResultSet] = toFuture(session.executeAsync(statement))

  }

  private[cassie] class Settings(config: Config) {

    def hosts: List[InetAddress] = if (config.hasPath("cassie.hosts"))
        config.getStringList("cassie.hosts").asScala.map(host => InetAddress.getByName(host)).toList
      else
        List(InetAddress.getLocalHost)

    def port: Int = if (config.hasPath("cassie.port"))
      config.getInt("cassie.port")
    else 9042

  }
}
