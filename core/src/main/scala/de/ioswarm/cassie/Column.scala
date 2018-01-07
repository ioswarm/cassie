package de.ioswarm.cassie

/**
  * Created by andreas on 20.08.17.
  */
object Column {
  def apply(name: String): Column = ColumnDef(name)
}
trait Column extends Columns with Expression with Conditional {

  def name: String
  def alias: Option[String]
  def partitionKey: Boolean
  def clustering: Boolean
  def clusteringOrder: Order
  def indexes: Seq[String]
  def static: Boolean
  def frozen: Boolean

  def alias(a: String): Column
  def partitionKey(pkey: Boolean): Column
  def clustering(ckey: Boolean): Column
  def clustering(order: Order): Column = clustering(true).clusteringOrder(order)
  def clusteringOrder(order: Order): Column
  def order(order: Order): Column = clusteringOrder(order)
  def static(stc: Boolean): Column
  def frozen(fro: Boolean): Column
  def index(name: String): Column

  def pk(): Column = partitionKey(true)
  def asc(): Column = clustering(ASC)
  def desc(): Column = clustering(DESC)

  def definition: String = name

  def cql: String = name+(alias match {
    case Some(a) => " AS "+a
    case None => ""
  })

  override def expression: Expression = this

}

case class ColumnDef(name: String
                     , alias: Option[String] = None
                     , partitionKey: Boolean = false
                     , clustering: Boolean = false
                     , clusteringOrder: Order = NONE
                     , indexes: Seq[String] = Seq.empty[String]
                     , static: Boolean = false
                     , frozen: Boolean = false
                    ) extends Column {

  def columns: List[Column] = List.empty[Column]

  def alias(a: String): ColumnDef = copy(alias = Some(a))
  def partitionKey(pkey: Boolean): ColumnDef = copy(partitionKey = pkey)
  def clustering(ckey: Boolean): ColumnDef = copy(clustering = ckey)
  def clusteringOrder(order: Order): ColumnDef = copy(clusteringOrder = order, clustering = order != NONE)
  def static(stc: Boolean): ColumnDef = copy(static = stc)
  def frozen(fro: Boolean): ColumnDef = copy(frozen = fro)

  def index(name: String): ColumnDef = copy(indexes = name +: this.indexes)

  def ::(col: Column): Columns = Columns(col, this)
  def withColumn(col: Column): Columns = ::(col)
}

case class TypedColumnDef[T](
                           name: String
                           , alias: Option[String] = None
                           , partitionKey: Boolean = false
                           , clustering: Boolean = false
                           , clusteringOrder: Order = NONE
                           , indexes: Seq[String] = Seq.empty[String]
                           , static: Boolean = false
                           , frozen: Boolean = false
                         )(implicit cf: CassandraFormat[T]) extends Column {
  def columns: List[Column] = List.empty[Column]

  def alias(a: String): TypedColumnDef[T] = copy(alias = Some(a))
  def partitionKey(pkey: Boolean): TypedColumnDef[T] = copy(partitionKey = pkey)
  def clustering(ckey: Boolean): TypedColumnDef[T] = copy(clustering = ckey)
  def clusteringOrder(order: Order): TypedColumnDef[T] = copy(clusteringOrder = order, clustering = order != NONE)
  def static(stc: Boolean): TypedColumnDef[T] = copy(static = stc)
  def frozen(fro: Boolean): TypedColumnDef[T] = copy(frozen = fro)

  def index(name: String): TypedColumnDef[T] = copy(indexes = name +: this.indexes)

  def ::(col: Column): Columns = Columns(col, this)
  def withColumn(col: Column): Columns = ::(col)

  def format: CassandraFormat[T] = cf
  override def definition: String = name+" "+(if(frozen || cf.customType) "FROZEN " else "")+cf.cqlType+(if (static) " STATIC" else "")
}

object Columns {
  def apply(columns: Column*): Columns = ColumnsDef(columns.toList)
}
trait Columns {
  def apply(index: Int): Column = columns(index)

  def columns: List[Column]

  def ::(col: Column): Columns

  def from(keyspace: String, table: String): SelectStatement = StatementBuilder.select(keyspace, table, columns, None)
  def from(from: String): SelectStatement = StatementBuilder.select(from, columns, None)

}

case class ColumnsDef(columns: List[Column]) extends Columns {

  def ::(c: Column): Columns = copy(columns = c +: this.columns)
  def withColumn(col: Column): Columns = ::(col)

}
