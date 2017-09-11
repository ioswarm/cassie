package de.ioswarm.cassie

/**
  * Created by andreas on 20.08.17.
  */
private[cassie] trait TerminationCondition extends Expression {
  def left: Expression
  def operator: String

  def cql: String = left.cql+" "+operator

  def paramValues: Seq[ValueExpression[_]] = {
    val ret = Seq.empty[ValueExpression[_]]
    left match {
      case c: Condition => c.paramValues(ret)
      case v: ValueExpression[_] => ret :+ v
      case _ => ret
    }
  }
}

case class AllowFilteringCondition(left: Expression) extends TerminationCondition {
  val operator: String = "ALLOW FILTERING"
}

case class LimitCondition(left: Expression, limit: Int) extends TerminationCondition {
  val operator: String = "LIMIT"

  override def cql: String = left.cql+" "+operator+" "+limit
}

private[cassie] trait Condition extends TerminationCondition {

  def right: Expression


  def AND(expression: Expression) = AndCondition(this, expression)
  def &&(expression: Expression) = AND(expression)

  def ALLOW_FILTERING() = AllowFilteringCondition(this)
  def AF() = ALLOW_FILTERING()

  def LIMIT(limit: Int) = LimitCondition(this, limit)

  override def cql: String = left.cql+" "+operator+" "+right.cql

  final def paramValues(values: Seq[ValueExpression[_]]): Seq[ValueExpression[_]] = {
    val ret = right match {
      case v: ValueExpression[_] => values :+ v
      case c: Condition => c.paramValues(values)
      case _ => values
    }

    left match {
      case c: Condition => c.paramValues(ret)
      case v: ValueExpression[_] => ret :+ v
      case _ => ret
    }
  }

  override def paramValues: Seq[ValueExpression[_]] = {
    paramValues(Seq.empty[ValueExpression[_]])
  }

}

trait Conditional extends Expression {
  def expression: Expression

  protected def typeCheck[T](value: T)(implicit writer: Writer[T]): Expression = value match {
    case e: Expression => e
    case _ => ValueExpression(value)
  }

  def EQ[T](value: T)(implicit writer: Writer[T]): Condition = EqualCondition(expression, typeCheck(value))
  def =:=[T](value: T)(implicit writer: Writer[T]): Condition = EQ(value)

  def IN[T](values: T*)(implicit writer: Writer[T]): Condition = InCondition(expression, GroupExpression(values.map(v => typeCheck(v))))

  def GT[T](value: T)(implicit writer: Writer[T]): Condition = GreaterThanCondition(expression, typeCheck(value))
  def >:>[T](value: T)(implicit writer: Writer[T]): Condition = GT(value)

  def GE[T](value: T)(implicit writer: Writer[T]): Condition = GreaterOrEqualCondition(expression, typeCheck(value))
  def >:=[T](value: T)(implicit writer: Writer[T]): Condition = GE(value)

  def LT[T](value: T)(implicit writer: Writer[T]): Condition = LessThanCondition(expression, typeCheck(value))
  def <:<[T](value: T)(implicit writer: Writer[T]): Condition = LT(value)

  def LE[T](value: T)(implicit writer: Writer[T]): Condition = LessOrEqualCondition(expression, typeCheck(value))
  def <:=[T](value: T)(implicit writer: Writer[T]): Condition = LE(value)

  def CONTAINS[T](value: T)(implicit writer: Writer[T]): Condition = ContainsCondition(expression, typeCheck(value))
  def CONTAINSKEY[T](value: T)(implicit writer: Writer[T]): Condition = ContainsKeyCondition(expression, typeCheck(value))
}

case class AndCondition(left: Expression, right: Expression) extends Condition with Conditional {
  val operator = "AND"
  val expression: Expression = this
}

case class EqualCondition(left: Expression, right: Expression) extends Condition{
  def operator = "="
}

case class InCondition(left: Expression, right: Expression) extends Condition{
  def operator = "IN"
}

case class GreaterThanCondition(left: Expression, right: Expression) extends Condition{
  def operator = ">"
}

case class GreaterOrEqualCondition(left: Expression, right: Expression) extends Condition{
  def operator = ">="
}

case class LessThanCondition(left: Expression, right: Expression) extends Condition{
  def operator = "<"
}

case class LessOrEqualCondition(left: Expression, right: Expression) extends Condition{
  def operator = "<="
}

case class ContainsCondition(left: Expression, right: Expression) extends Condition{
  def operator = "CONTAINS"
}

case class ContainsKeyCondition(left: Expression, right: Expression) extends Condition{
  def operator = "CONTAINS KEY"
}
