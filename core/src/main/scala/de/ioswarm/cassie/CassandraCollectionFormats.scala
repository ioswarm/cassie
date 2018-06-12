package de.ioswarm.cassie

import scala.reflect.ClassTag

/**
  * Created by andreas on 25.08.17.
  */
trait CassandraCollectionFormats extends CassandraBasicFormats {

  implicit def listFormat[V](implicit format: CassandraFormat[V], ct: ClassTag[V]) = new CassandraFormat[List[V]] {
    import scala.collection.JavaConverters._

    override def toCql(t: List[V]): String = t.map(v => format.toCql(v)).mkString("[", ", ", "]")

    override def cqlType: String = "list<"+format+">"

    override def read(index: Int, stmt: Gettable): List[V] = stmt.getList(index, ct.runtimeClass).asScala.asInstanceOf[List[V]]

    override def read(name: String, stmt: Gettable): List[V] = stmt.getList(name, ct.runtimeClass).asScala.asInstanceOf[List[V]]

    override def write(name: String, t: List[V], stmt: Settable): Settable = stmt.setList(name, t.asJava)

    override def write(index: Int, t: List[V], stmt: Settable): Settable = stmt.setList(index, t.asJava)
  }

  implicit def setFormat[V](implicit format: CassandraFormat[V], ct: ClassTag[V]) = new CassandraFormat[Set[V]] {
    import scala.collection.JavaConverters._

    override def toCql(t: Set[V]): String = t.map(v => format.toCql(v)).mkString("{", ", ", "}")

    override def cqlType: String = "set<"+format.cqlType+">"

    override def read(index: Int, stmt: Gettable): Set[V] = stmt.getSet(index, ct.runtimeClass).asScala.asInstanceOf[Set[V]]

    override def read(name: String, stmt: Gettable): Set[V] = stmt.getSet(name, ct.runtimeClass).asScala.asInstanceOf[Set[V]]

    override def write(name: String, t: Set[V], stmt: Settable): Settable = stmt.setSet(name, t.asJava)

    override def write(index: Int, t: Set[V], stmt: Settable): Settable = stmt.setSet(index, t.asJava)
  }

  implicit def mapFormat[K, V](implicit kformat: CassandraFormat[K], vformat: CassandraFormat[V], ctk: ClassTag[K], ctv: ClassTag[V]) = new CassandraFormat[Map[K, V]] {
    import scala.collection.JavaConverters._

    override def toCql(t: Map[K, V]): String = t.map(e => kformat.toCql(e._1)+": "+vformat.toCql(e._2)).mkString("{", ", ", "}")

    override def cqlType: String = "map<"+kformat.cqlType+", "+vformat.cqlType+">"

    override def read(index: Int, stmt: Gettable): Map[K, V] = stmt.getMap(index, ctk.runtimeClass, ctv.runtimeClass).asScala.toMap.asInstanceOf[Map[K,V]]

    override def read(name: String, stmt: Gettable): Map[K, V] = stmt.getMap(name, ctk.runtimeClass, ctv.runtimeClass).asScala.toMap.asInstanceOf[Map[K,V]]

    override def write(name: String, t: Map[K, V], stmt: Settable): Settable = stmt.setMap(name, t.asJava)

    override def write(index: Int, t: Map[K, V], stmt: Settable): Settable = stmt.setMap(index, t.asJava)
  }

}
