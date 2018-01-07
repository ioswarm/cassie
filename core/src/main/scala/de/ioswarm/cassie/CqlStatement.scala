package de.ioswarm.cassie

import com.datastax.driver.core.ResultSet
import de.ioswarm.cassie.Cluster.Connection

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Created by andreas on 27.08.17.
  */
object CqlStatement {

  def apply(cql: String): CqlStatement = DefaultQuery(cql)

  case class DefaultQuery(cql: String) extends CqlStatement

}
trait CqlStatement {

  def cql: String

  def execute(implicit connection: Connection): ResultSet = connection.execute(cql)
  def executeAsync(implicit connection: Connection): Future[ResultSet] = connection.executeAsync(cql)

  def execute[K](implicit connection: Connection, f: QueryReader[K]): Vector[K] = {
    import scala.collection.JavaConverters._

    execute(connection).asScala.map(r => f(Gettable(r))).toVector
  }
  def executeAsync[K](implicit connection: Connection, ex: ExecutionContext, f: QueryReader[K]): Future[Vector[K]] = {
    import scala.collection.JavaConverters._

    executeAsync(connection).map(res => res.asScala.map(row => f(Gettable(row))).toVector)
  }

  def execute[K](ks: K*)(implicit connection: Connection, f: QueryWriter[K]): Unit = {
    val pstmt = connection.prepare(cql)
    val bind = pstmt.bind()
    val settable = Settable(bind)
    ks.foreach { k =>
      f(k, settable)
      connection.execute(bind)
    }
  }

  def executeAsync[K](ks: K*)(implicit connection: Connection, ex: ExecutionContext, f: QueryWriter[K]): Future[Unit] = {
    val pstmt = connection.prepare(cql)
    val p = Promise[Unit]
    Future.sequence(ks.map(k => {
      val bind = pstmt.bind()
      val settable = Settable(bind)
      f(k, settable)
      connection.executeAsync(bind)
    })) onComplete {
      case Success(_) => p.success(Unit)
      case Failure(e) => p.failure(e)
    }
    p.future
  }

  override def toString: String = cql

}

