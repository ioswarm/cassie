package de.ioswarm.cassie

import com.datastax.driver.core.ResultSet
import de.ioswarm.cassie.Cluster.Connection

import scala.concurrent.{ExecutionContext, Future}

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
  def executeAsync(implicit connection: Connection, ex: ExecutionContext): Future[ResultSet] = Future{
    connection.execute(cql)
  }(ex)

  def execute[K](implicit connection: Connection, f: QueryReader[K]): Vector[K] = {
    import scala.collection.JavaConversions._

    execute(connection).map(r => f(Gettable(r))).toVector
  }
  def executeAsync[K](implicit connection: Connection, ex: ExecutionContext, f: QueryReader[K]): Future[Vector[K]] = {
    import scala.collection.JavaConversions._

    executeAsync(connection, ex).map(res => res.map(row => f(Gettable(row))).toVector)
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

  def executeAsync[K](ks: K*)(implicit connection: Connection, ex: ExecutionContext, f: QueryWriter[K]): Future[Unit] = Future{
    execute(ks :_*)(connection, f)
  }(ex)

  override def toString: String = cql

}

