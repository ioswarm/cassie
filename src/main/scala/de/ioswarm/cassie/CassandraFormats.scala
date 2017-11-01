package de.ioswarm.cassie

import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.{Time, Timestamp, Date => SQLDate}
import java.util.{Date, UUID}

/**
  * Created by andreas on 24.08.17.
  */
trait CassandraFormats extends CassandraTupleFormats {

  implicit def optionFormat[T](implicit format: CassandraFormat[T]) = new CassandraFormat[Option[T]] {

    override def customType: Boolean = format.customType

    override def toCql(t: Option[T]): String = t match {
      case Some(v) => format.toCql(v)
      case None => "null"
    }

    override def cqlType: String = format.cqlType

    override def read(index: Int, stmt: Gettable): Option[T] = {
      if (stmt.isNull(index)) None
      else Some(format.read(index, stmt))
    }

    override def read(name: String, stmt: Gettable): Option[T] = {
      if (stmt.isNull(name)) None
      else Some(format.read(name, stmt))
    }

    override def write(name: String, t: Option[T], stmt: Settable): Settable = t match {
      case Some(v) => format.write(name, v, stmt)
      case None => stmt.setToNull(name)
    }

    override def write(index: Int, t: Option[T], stmt: Settable): Settable = t match {
      case Some(v) => format.write(index, v, stmt)
      case None => stmt.setToNull(index)
    }
  }

  def toCql(a: Any): String = a match {
    case u: UUID => u.toCql
    case t: Timestamp => t.toCql
    case bi: BigInt => bi.toCql
    case inet: InetAddress => inet.toCql
    case barr: Array[Byte] => barr.toCql
    case bbuf: ByteBuffer => bbuf.toCql
    case f: Float => f.toCql
    case l: Long => l.toCql
    case b: Boolean => b.toCql
    case t: Time => t.toCql
    case b: Byte => b.toCql
    case d: BigDecimal => d.toCql
    case s: Short => s.toCql
    case d: SQLDate => d.toCql
    case d: Double => d.toCql
    case d: Date => d.toCql
    case i: Int => i.toCql
    case s: String => s.toCql

    case l: List[_] => l.map(a => toCql(a)).mkString("[", ", ", "]")
    case s: Set[_] => s.map(a => toCql(a)).mkString("{", ", ", "}")
    case m: Map[_, _] => m.map(t => toCql(t._1)+": "+toCql(t._2)).mkString("{", ", ", "}")

    case o: Option[_] => o match {
      case Some(v) => toCql(v)
      case None => "null"
    }

    case p: Product => p.productIterator.map(a => toCql(a)).mkString("(", ", ", ")")

  }

}
