package de.ioswarm.cassie

/**
  * Created by andreas on 27.08.17.
  */
object RowValue {

  def apply[T](index: Int, value: T): RowValue[T] = IndexRowValue(index, Some(value))
  def apply[T](name: String, value: T): RowValue[T] = NamedRowValue(name, Some(value))

  def apply[T](index: Int, value: Option[T]): RowValue[T] = IndexRowValue(index, value)
  def apply[T](name: String, value: Option[T]): RowValue[T] = NamedRowValue(name, value)


  case class IndexRowValue[T](index: Int, value: Option[T]) extends RowValue[T] {
    override def write(settable: Settable)(implicit writer: Writer[T]): Settable = value match {
      case Some(v) => writer.write(index, v, settable)
      case None => settable.setToNull(index)
    }
  }

  case class NamedRowValue[T](name: String, value: Option[T]) extends RowValue[T] {
    override def write(settable: Settable)(implicit writer: Writer[T]): Settable = value match {
      case Some(v) => writer.write(name, v, settable)
      case None => settable.setToNull(name)
    }
  }
}
trait RowValue[T] {

  def value: Option[T]

  def write(settable: Settable)(implicit writer: Writer[T]): Settable

}