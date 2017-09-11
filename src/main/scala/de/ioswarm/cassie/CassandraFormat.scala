package de.ioswarm.cassie

/**
  * Created by andreas on 23.08.17.
  */
trait CassandraFormat[T] extends Reader[T] with Writer[T] {
  def toCql(t: T): String
  def cqlType: String

  def customType: Boolean = false
}

abstract class CassandraBasicFormat[T](val cqlType: String) extends CassandraFormat[T] {
  override def toCql(t: T): String = t.toString
}

trait CassandraTupleFormat[T] extends CassandraFormat[T] {

}