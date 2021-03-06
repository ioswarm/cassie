package de.ioswarm.cassie

import com.datastax.driver.core.ResultSet
import de.ioswarm.cassie.Cluster.Connection

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Created by andreas on 31.08.17.
  */
object StatementBuilder {

  private val fromR = "^([a-zA-Z][a-zA-Z0-9_]*)(\\.([a-zA-Z][a-zA-Z0-9]*))?$".r

  def select(keyspace: String, table: String, columns: Seq[Column], condition: Option[TerminationCondition]): SelectStatement = {
    SelectStatement(Some(keyspace), table, columns, condition)
  }

  def select(from: String, columns: Seq[Column], condition: Option[TerminationCondition]): SelectStatement = {
    require(from matches fromR.regex)

    val ft = from match {
      case fromR(keyspace,_,table) if table != null => Some(keyspace) -> table
      case fromR(table, _, _) => None -> table
    }
    SelectStatement(ft._1, ft._2, columns, condition)
  }

  def from[K](implicit ct: ClassTag[K]): TypedSelectStatement[K] = TypedSelectStatement[K](None, None, None)(ct)

  def from[K](from: String)(implicit ct: ClassTag[K]): TypedSelectStatement[K] = {
    require(from matches fromR.regex)

    val ft = from match {
      case fromR(keyspace,_,table) if table != null => Some(keyspace) -> Some(table)
      case fromR(table, _, _) => None -> Some(table)
      case _ => None -> None
    }
    TypedSelectStatement[K](ft._1, ft._2, None)(ct)
  }

  def from[K](keyspace: String, table: String)(implicit ct: ClassTag[K]): TypedSelectStatement[K] = TypedSelectStatement(Some(keyspace), Some(table), None)(ct)


  def table[T](implicit tbl: Table[T]): TableStatement[T] = TableStatement(None)(tbl)
}

case class SelectStatement(keyspace: Option[String], table: String, columns: Seq[Column], condition: Option[TerminationCondition]) {

  def cql: String = "SELECT "+(if (columns.nonEmpty) columns.map(c => c.cql).mkString(", ") else "*")+" FROM "+ {
      keyspace match {
        case Some(k) => k + "." + table
        case None => table
      }
    } + {
    condition match {
      case Some(c) => " WHERE "+c.cql
      case None => ""
    }
  }

  def ::(col: Column): SelectStatement = copy(columns = col +: this.columns)
  def withColumn(col: Column): SelectStatement = ::(col)

  def where(tcondition: TerminationCondition): SelectStatement = copy(condition = Some(tcondition))

  def as[K]()(implicit connection: Connection, f: QueryReader[K]): Vector[K] = {
    val pstmt = connection.prepare(cql)
    val bstmt = pstmt.bind()
    for (pval <- condition.map(tc => tc.paramValues).getOrElse(Seq.empty[ValueExpression[_]]))
      pval.write(Settable(bstmt))

    import scala.collection.JavaConverters._

    connection.execute(bstmt).asScala.map(r => f(Gettable(r))).toVector
  }

  def head[K]()(implicit connection: Connection, f: QueryReader[K]): K = as[K]()(connection, f).head
  def headOption[K]()(implicit connection: Connection, f: QueryReader[K]): Option[K] = as[K]()(connection, f).headOption
  def last[K]()(implicit connection: Connection, f: QueryReader[K]): K = as[K]()(connection, f).last
  def lastOption[K]()(implicit connection: Connection, f: QueryReader[K]): Option[K] = as[K]()(connection, f).lastOption

  override def toString: String = cql

}

case class TypedSelectStatement[T](keyspace: Option[String], table: Option[String], condition: Option[TerminationCondition])(implicit ct: ClassTag[T]) {

  def cql: String = "SELECT * FROM "+ {
      def tableName: String = table match {
        case Some(tbl) => tbl
        case None => ct.runtimeClass.getSimpleName
      }
      keyspace match {
        case Some(k) => k + "." + tableName
        case None => tableName
      }
    } + {
      condition match {
        case Some(c) => " WHERE "+c.cql
        case None => ""
      }
    }

  def where(tcondition: TerminationCondition): TypedSelectStatement[T] = copy(condition = Some(tcondition))

  private[cassie] def fetch(stmt: String)(implicit connection: Connection, f: QueryReader[T]): Vector[T] = {
    val pstmt = connection.prepare(cql)
    val bstmt = pstmt.bind()
    for (pval <- condition.map(tc => tc.paramValues).getOrElse(Seq.empty[ValueExpression[_]]))
      pval.write(Settable(bstmt))

    import scala.collection.JavaConverters._

    connection.execute(bstmt).asScala.map(r => f(Gettable(r))).toVector
  }

  def fetch(implicit connection: Connection, f: QueryReader[T]): Vector[T] = fetch(cql)(connection, f)
  def list(implicit connection: Connection, f: QueryReader[T]): Vector[T] = fetch(connection, f)

  def head(implicit connection: Connection, f: QueryReader[T]): T = fetch(cql+" LIMIT 1")(connection, f).head
  def headOption(implicit connection: Connection, f: QueryReader[T]): Option[T] = fetch(cql+" LIMIT 1")(connection, f).headOption

  def last(implicit connection: Connection, f: QueryReader[T]): T = fetch(connection, f).last
  def lastOption(implicit connection: Connection, f: QueryReader[T]): Option[T] = fetch(connection, f).lastOption

  override def toString: String = cql

}

case class TableStatement[T](condition: Option[TerminationCondition])(implicit tbl: Table[T]) {

  def where(tcondition: TerminationCondition): TableStatement[T] = copy(condition = Some(tcondition))

  def cqlSelect: String = tbl.columns.mkString("SELECT ", ", ", " FROM ")+ tbl.qualifiedName + {
    condition match {
      case Some(c) => " WHERE "+c.cql
      case None => ""
    }
  }

  def cqlInsert: String = tbl.columns.mkString("INSERT INTO "+tbl.qualifiedName+" (", ", ", ")")+tbl.columns.mkString(" VALUES (:", ", :", ")")

  def cqlUpdate: String = "UPDATE "+tbl.qualifiedName+" SET "+tbl.valueColumns.map(c => c.name+" = :"+c.name).mkString(", ")+" WHERE "+tbl.primaryColumns.map(c => c.name+" = :"+c.name).mkString(" AND ")

  def cqlDelete: String = "DELETE FROM "+tbl.qualifiedName+" WHERE "+tbl.primaryColumns.map(c => c.name+" = :"+c.name).mkString(" AND ")

  def cqlCreate: String = String.format("CREATE TABLE IF NOT EXISTS %s (%s%s) %s"
    , tbl.qualifiedName
    , tbl.columns.map(c => c.definition).mkString(", ")
    , /*if (tbl.primaryColumns.nonEmpty)*/ String.format(", PRIMARY KEY (%s%s)"
      , tbl.partitionKeys.map(c => c.name).mkString(if (tbl.partitionKeys.size > 1) "(" else "", ", ", if (tbl.partitionKeys.size > 1) ")" else "")
      , if (tbl.clusteringColumns.nonEmpty) tbl.clusteringColumns.map(c => c.name).mkString(", ", ", ", "") else ""
    ) /*else ""*/
    , if (tbl.orderedColumns.nonEmpty) tbl.orderedColumns.map(c => c.name+" "+c.clusteringOrder).mkString("WITH CLUSTERING ORDER BY (", ", ", ")") else ""
  )

  def cqlCreateIndex(name: String, columns: Column*): String = "CREATE INDEX IF NOT EXISTS "+tbl.name+"_"+name+" ON "+tbl.qualifiedName+columns.mkString(" (", ", ", ")")

  def cqlDrop: String = "DROP TABLE "+tbl.qualifiedName

  def cqlTruncate: String = "TRUNCATE TABLE "+tbl.qualifiedName

  private def execute(cql: String)(implicit connection: Connection): ResultSet = connection.execute(cql)

  private def execute(cql: String, v: T)(implicit connection: Connection): T = {
    val pstmt = connection.prepare(cql)
    val bind = pstmt.bind()
    val settable = Settable(bind)
    tbl(v, settable)
    connection.execute(bind)
    v
  }

  private def execute(cql: String, vals: Seq[T])(implicit connection: Connection): Seq[T] = {
    val pstmt = connection.prepare(cql)
    val bind = pstmt.bind()
    val settable = Settable(bind)
    vals.foreach { k =>
      tbl(k, settable)
      connection.execute(bind)
    }
    vals
  }

  private def executeAsync(cql: String)(implicit connection: Connection): Future[ResultSet] = connection.executeAsync(cql)

  private def executeAsync(cql: String, v: T)(implicit connection: Connection, ex: ExecutionContext): Future[T] = {
    val pstmt = connection.prepare(cql)
    val bind = pstmt.bind()
    val settable = Settable(bind)
    tbl(v, settable)
    connection.executeAsync(bind).map(_ => v)
  }

  private def executeAsync(cql: String, vals: Seq[T])(implicit connection: Connection, ex: ExecutionContext): Future[Seq[T]] = {
    val pstmt = connection.prepare(cql)
    Future.sequence(vals.map{v =>
      val bind = pstmt.bind()
      val settable = Settable(bind)
      tbl(v, settable)
      connection.executeAsync(bind).map(_ => v)
    })
  }

  private def fetch(stmt: String)(implicit connection: Connection): Vector[T] = {
    val pstmt = connection.prepare(cqlSelect)
    val bstmt = pstmt.bind()
    for (pval <- condition.map(tc => tc.paramValues).getOrElse(Seq.empty[ValueExpression[_]]))
      pval.write(Settable(bstmt))

    import scala.collection.JavaConverters._

    connection.execute(bstmt).asScala.map(r => tbl(Gettable(r))).toVector
  }

  def fetch(implicit connection: Connection): Vector[T] = fetch(cqlSelect)(connection)
  def list(implicit connection: Connection): Vector[T] = fetch(connection)
  def select(implicit connection: Connection): Vector[T] = fetch(connection)

  def head(implicit connection: Connection): T = fetch(cqlSelect+" LIMIT 1")(connection).head
  def headOption(implicit connection: Connection): Option[T] = fetch(cqlSelect+" LIMIT 1")(connection).headOption

  def last(implicit connection: Connection): T = fetch(connection).last
  def lastOption(implicit connection: Connection): Option[T] = fetch(connection).lastOption

  def insert(t: T)(implicit connection: Connection): T = execute(cqlInsert, t)(connection)
  def insert(t: T, ts: T*)(implicit connection: Connection): Seq[T] = execute(cqlInsert, t +: ts)(connection)

  def insertAsync(t: T)(implicit connection: Connection, ex: ExecutionContext): Future[T] = executeAsync(cqlInsert, t)(connection, ex)
  def insertAsync(t: T, ts: T*)(implicit connection: Connection, ex: ExecutionContext): Future[Seq[T]] = executeAsync(cqlInsert, t +: ts)(connection, ex)

  def update(t: T)(implicit connection: Connection): T = execute(cqlUpdate, t)(connection)
  def update(t: T, ts: T*)(implicit connection: Connection): Seq[T] = execute(cqlUpdate, t +: ts)(connection)

  def updateAsync(t: T)(implicit connection: Connection, ex: ExecutionContext): Future[T] = executeAsync(cqlUpdate, t)(connection, ex)
  def updateAsync(t: T, ts: T*)(implicit connection: Connection, ex: ExecutionContext): Future[Seq[T]] = executeAsync(cqlUpdate, t +: ts)(connection, ex)

  def upsert(t: T)(implicit connection: Connection): T = update(t)(connection)
  def upsert(t: T, ts: T*)(implicit connection: Connection): Seq[T] = update(t, ts :_*)(connection)

  def upsertAsync(t: T)(implicit connection: Connection, ex: ExecutionContext): Future[T] = updateAsync(t)(connection, ex)
  def upsertAsync(t: T, ts: T*)(implicit connection: Connection, ex: ExecutionContext): Future[Seq[T]] = updateAsync(t, ts :_*)(connection, ex)

  def delete(t: T)(implicit connection: Connection): T = execute(cqlDelete, t)(connection)
  def delete(t: T, ts: T*)(implicit connection: Connection): Seq[T] = execute(cqlDelete, t +: ts)(connection)

  def deleteAsync(t: T)(implicit connection: Connection, ex: ExecutionContext): Future[T] = executeAsync(cqlDelete, t)(connection, ex)
  def deleteAsync(t: T, ts: T*)(implicit connection: Connection, ex: ExecutionContext): Future[Seq[T]] = executeAsync(cqlDelete, t +: ts)(connection, ex)

  def create(implicit connection: Connection): Unit = {
    execute(cqlCreate)(connection)
    tbl.indexedColumns.flatMap(c => c.indexes.map(n => n -> c)).groupBy(_._1).map { case (k,v) => (k,v.map(_._2)) }.foreach{
      case (k, v) => execute(cqlCreateIndex(k, v :_*))(connection)
    }
  }

  def drop(implicit connection: Connection): Unit = execute(cqlDrop)(connection)

  def truncate(implicit connection: Connection): Unit = execute(cqlTruncate)(connection)

}

private[cassie] case class TypeBuilder[T]()(implicit tpe: Type[T]) {

  def cqlCreate: String = String.format("CREATE TYPE IF NOT EXISTS %s (%s)"
    , tpe.qualifiedName
    , tpe.columns.map(c => c.definition).mkString(", ")
  )

  def cqlDrop: String = "DROP TYPE "+tpe.qualifiedName

  def create(implicit connection: Connection): Unit = connection.execute(cqlCreate)

  def drop(implicit connection: Connection): Unit = connection.execute(cqlDrop)

}