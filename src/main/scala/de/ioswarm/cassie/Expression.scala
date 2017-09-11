package de.ioswarm.cassie

import scala.util.Random

/**
  * Created by andreas on 20.08.17.
  */
private[cassie] trait Expression extends CqlSupport {

}

case class GroupExpression(expressions: Seq[Expression]) extends Expression {
  def cql: String = expressions.map(e => e.cql).mkString("(", ", ", ")")
}

case class ValueExpression[T](value: T)(implicit writer: Writer[T]) extends Expression {
  val param: String = "value_"+Random.nextInt(5000).abs

  def cql: String = ":"+param

  def write(stmt: Settable): Settable = writer.write(param, value, stmt)
}
