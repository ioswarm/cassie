package de.ioswarm

import java.nio.ByteBuffer
import java.sql.{Time, Timestamp, Date => SQLDate}
import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs
import de.ioswarm.cassie.Cluster.Connection

import scala.reflect.ClassTag

/**
  * Created by andreas on 23.08.17.
  */
package object cassie extends CassandraFormats {

  type QueryReader[T] = Gettable => T
  type QueryWriter[T] = (T, Settable) => Settable

  implicit class ExtendClasses[T](t: T) {
    def cqlType(implicit format: CassandraFormat[T]): String = format.cqlType
    def toCql(implicit format: CassandraFormat[T]): String = format.toCql(t)
  }

  implicit class ExtendProducts[T <: Product](t: T) {
    def insert(implicit connection: Connection, tbl: Table[T]): Unit = table(tbl).insert(t)(connection)
    def update(implicit connection: Connection, tbl: Table[T]): Unit = table(tbl).update(t)(connection)
    def upsert(implicit connection: Connection, tbl: Table[T]): Unit = table(tbl).upsert(t)(connection)
    def delete(implicit connection: Connection, tbl: Table[T]): Unit = table(tbl).delete(t)(connection)
  }

  implicit class ExtendIntClass(i: Int) {
    def :=[T](t: T): RowValue[T] = RowValue(i, t)
  }

  implicit class ExtendStringClass(s: String) {
    def :=[T](t: T): RowValue[T] = RowValue(s, t)
  }

  implicit class CqlHelper(private val sc: StringContext) extends AnyVal {
    def cql(args: Any*): CqlStatement = {
      val strings = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuilder(strings.next())
      while (strings.hasNext) {
        buf append toCql(expressions.next())
        buf append strings.next()
      }
      CqlStatement(buf.toString())
    }
  }

  implicit class ByteArrayToHex(arr: Array[Byte]) {
    private final val HEX_DIGITS = Array('0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F')

    def toHex: String = arr.map(b => "" + HEX_DIGITS((b >> 4) & 0xF) + HEX_DIGITS(b & 0xF)).mkString("0x", "", "")
  }

  implicit class ByteBufferToHex(buffer: ByteBuffer) {

    def toHex: String = {
      val arr = new Array[Byte](buffer.remaining())
      arr.toHex
    }

  }

  implicit class DateExtender(d: Date) {
    def toUuid: UUID = UUIDs.startOf(d.getTime)
  }

  implicit class UuidExtender(uuid: UUID) {
    def unixTimestamp: Long = UUIDs.unixTimestamp(uuid)
    def toDate: Date = new Date(unixTimestamp)
    def toTime: Time = new Time(unixTimestamp)
    def toTimestamp: Timestamp = new Timestamp(unixTimestamp)
    def toSqlDate: SQLDate = new SQLDate(unixTimestamp)
  }

  implicit class ColumnHelper(private val sc: StringContext) extends AnyVal {
    def $(args: Any*): Column = {
      val strings = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuffer(strings.next())
      while (strings.hasNext) {
        buf append expressions.next()
        buf append strings.next()
      }
      ColumnDef(buf.toString)
    }
  }

  implicit class ColumnExtender(c: Column) {
    def as[K](implicit cf: CassandraFormat[K]): TypedColumnDef[K] = TypedColumnDef(
      c.name
      , c.alias
      , c.partitionKey
      , c.clustering
      , c.clusteringOrder
      , c.indexes
      , c.static
      , if (cf.customType) true else c.frozen
    )(cf)
  }

  def from[K](implicit ct: ClassTag[K]): TypedSelectStatement[K] = StatementBuilder.from(ct)

  def from[K](from: String)(implicit ct: ClassTag[K]): TypedSelectStatement[K] = StatementBuilder.from(from)(ct)

  def from[K](keyspace: String, table: String)(implicit ct: ClassTag[K]): TypedSelectStatement[K] = StatementBuilder.from(keyspace, table)(ct)


  def table[T](implicit tbl: Table[T]): TableStatement[T] = StatementBuilder.table(tbl)


}
