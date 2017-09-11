package de.ioswarm.cassie

import java.math.{BigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.datastax.driver.core._
import com.google.common.reflect.TypeToken

/**
  * Created by andreas on 27.08.17.
  */
object Settable {
  def apply(bstmt: BoundStatement): Settable = new BoundStatementSettable(bstmt)
  def apply(udt: UDTValue): Settable = new UDTValueSettable(udt)
}

abstract class Settable extends SettableData[Settable] {
  def getDelegate[T]: T
}

class BoundStatementSettable(val st: BoundStatement) extends Settable {

  def statement: BoundStatement = st

  override def setByte(i: Int, v: Byte): Settable = {
    st.setByte(i,v)
    this
  }

  override def setBytes(i: Int, v: ByteBuffer): Settable = {
    st.setBytes(i, v)
    this
  }

  override def set[V](i: Int, v: V, targetClass: Class[V]): Settable = {
    st.set(i, v, targetClass)
    this
  }

  override def set[V](i: Int, v: V, targetType: TypeToken[V]): Settable = {
    st.set(i, v, targetType)
    this
  }

  override def set[V](i: Int, v: V, codec: TypeCodec[V]): Settable = {
    st.set(i, v, codec)
    this
  }

  override def setDate(i: Int, v: LocalDate): Settable = {
    st.setDate(i, v)
    this
  }

  override def setTimestamp(i: Int, v: Date): Settable = {
    st.setTimestamp(i, v)
    this
  }

  override def setUUID(i: Int, v: UUID): Settable = {
    st.setUUID(i, v)
    this
  }

  override def setUDTValue(i: Int, v: UDTValue): Settable = {
    st.setUDTValue(i, v)
    this
  }

  override def setString(i: Int, v: String): Settable = {
    st.setString(i, v)
    this
  }

  override def setFloat(i: Int, v: Float): Settable = {
    st.setFloat(i, v)
    this
  }

  override def setInet(i: Int, v: InetAddress): Settable = {
    st.setInet(i, v)
    this
  }

  override def setList[E](i: Int, v: util.List[E]): Settable = {
    st.setList(i, v)
    this
  }

  override def setList[E](i: Int, v: util.List[E], elementsClass: Class[E]): Settable = {
    st.setList(i, v, elementsClass)
    this
  }

  override def setList[E](i: Int, v: util.List[E], elementsType: TypeToken[E]): Settable = {
    st.setList(i, v, elementsType)
    this
  }

  override def setTupleValue(i: Int, v: TupleValue): Settable = {
    st.setTupleValue(i, v)
    this
  }

  override def setDecimal(i: Int, v: BigDecimal): Settable = {
    st.setDecimal(i, v)
    this
  }

  override def setDouble(i: Int, v: Double): Settable = {
    st.setDouble(i, v)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V]): Settable = {
    st.setMap(i, v)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V], keysClass: Class[K], valuesClass: Class[V]): Settable = {
    st.setMap(i, v, keysClass, valuesClass)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V], keysType: TypeToken[K], valuesType: TypeToken[V]): Settable = {
    st.setMap(i, v, keysType, valuesType)
    this
  }

  override def setToNull(i: Int): Settable = {
    st.setToNull(i)
    this
  }

  override def setTime(i: Int, v: Long): Settable = {
    st.setTime(i, v)
    this
  }

  override def setBytesUnsafe(i: Int, v: ByteBuffer): Settable = {
    st.setBytesUnsafe(i, v)
    this
  }

  override def setShort(i: Int, v: Short): Settable = {
    st.setShort(i, v)
    this
  }

  override def setBool(i: Int, v: Boolean): Settable = {
    st.setBool(i, v)
    this
  }

  override def setLong(i: Int, v: Long): Settable = {
    st.setLong(i, v)
    this
  }

  override def setVarint(i: Int, v: BigInteger): Settable = {
    st.setVarint(i, v)
    this
  }

  override def setInt(i: Int, v: Int): Settable = {
    st.setInt(i, v)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E]): Settable = {
    st.setSet(i, v)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E], elementsClass: Class[E]): Settable = {
    st.setSet(i, v, elementsClass)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E], elementsType: TypeToken[E]): Settable = {
    st.setSet(i, v, elementsType)
    this
  }

  override def setByte(name: String, v: Byte): Settable = {
    st.setByte(name, v)
    this
  }

  override def setBytes(name: String, v: ByteBuffer): Settable = {
    st.setBytes(name, v)
    this
  }

  override def set[V](name: String, v: V, targetClass: Class[V]): Settable = {
    st.set(name, v, targetClass)
    this
  }

  override def set[V](name: String, v: V, targetType: TypeToken[V]): Settable = {
    st.set(name, v, targetType)
    this
  }

  override def set[V](name: String, v: V, codec: TypeCodec[V]): Settable = {
    st.set(name, v, codec)
    this
  }

  override def setDate(name: String, v: LocalDate): Settable = {
    st.setDate(name, v)
    this
  }

  override def setTimestamp(name: String, v: Date): Settable = {
    st.setTimestamp(name, v)
    this
  }

  override def setUUID(name: String, v: UUID): Settable = {
    st.setUUID(name, v)
    this
  }

  override def setUDTValue(name: String, v: UDTValue): Settable = {
    st.setUDTValue(name, v)
    this
  }

  override def setString(name: String, v: String): Settable = {
    st.setString(name, v)
    this
  }

  override def setFloat(name: String, v: Float): Settable = {
    st.setFloat(name, v)
    this
  }

  override def setInet(name: String, v: InetAddress): Settable = {
    st.setInet(name, v)
    this
  }

  override def setList[E](name: String, v: util.List[E]): Settable = {
    st.setList(name, v)
    this
  }

  override def setList[E](name: String, v: util.List[E], elementsClass: Class[E]): Settable = {
    st.setList(name, v, elementsClass)
    this
  }

  override def setList[E](name: String, v: util.List[E], elementsType: TypeToken[E]): Settable = {
    st.setList(name, v, elementsType)
    this
  }

  override def setTupleValue(name: String, v: TupleValue): Settable = {
    st.setTupleValue(name, v)
    this
  }

  override def setDecimal(name: String, v: BigDecimal): Settable = {
    st.setDecimal(name, v)
    this
  }

  override def setDouble(name: String, v: Double): Settable = {
    st.setDouble(name, v)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V]): Settable = {
    st.setMap(name, v)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V], keysClass: Class[K], valuesClass: Class[V]): Settable = {
    st.setMap(name, v, keysClass, valuesClass)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V], keysType: TypeToken[K], valuesType: TypeToken[V]): Settable = {
    st.setMap(name, v, keysType, valuesType)
    this
  }

  override def setToNull(name: String): Settable = {
    st.setToNull(name)
    this
  }

  override def setTime(name: String, v: Long): Settable = {
    st.setTime(name, v)
    this
  }

  override def setBytesUnsafe(name: String, v: ByteBuffer): Settable = {
    st.setBytesUnsafe(name, v)
    this
  }

  override def setShort(name: String, v: Short): Settable = {
    st.setShort(name, v)
    this
  }

  override def setBool(name: String, v: Boolean): Settable = {
    st.setBool(name, v)
    this
  }

  override def setLong(name: String, v: Long): Settable = {
    st.setLong(name, v)
    this
  }

  override def setVarint(name: String, v: BigInteger): Settable = {
    st.setVarint(name, v)
    this
  }

  override def setInt(name: String, v: Int): Settable = {
    st.setInt(name, v)
    this
  }

  override def setSet[E](name: String, v: util.Set[E]): Settable = {
    st.setSet(name, v)
    this
  }

  override def setSet[E](name: String, v: util.Set[E], elementsClass: Class[E]): Settable = {
    st.setSet(name, v, elementsClass)
    this
  }

  override def setSet[E](name: String, v: util.Set[E], elementsType: TypeToken[E]): Settable = {
    st.setSet(name, v, elementsType)
    this
  }

  def getDelegate[T]: T = st.asInstanceOf[T]
}

class UDTValueSettable(val st: UDTValue) extends Settable {

  override def setByte(i: Int, v: Byte): Settable = {
    st.setByte(i,v)
    this
  }

  override def setBytes(i: Int, v: ByteBuffer): Settable = {
    st.setBytes(i, v)
    this
  }

  override def set[V](i: Int, v: V, targetClass: Class[V]): Settable = {
    st.set(i, v, targetClass)
    this
  }

  override def set[V](i: Int, v: V, targetType: TypeToken[V]): Settable = {
    st.set(i, v, targetType)
    this
  }

  override def set[V](i: Int, v: V, codec: TypeCodec[V]): Settable = {
    st.set(i, v, codec)
    this
  }

  override def setDate(i: Int, v: LocalDate): Settable = {
    st.setDate(i, v)
    this
  }

  override def setTimestamp(i: Int, v: Date): Settable = {
    st.setTimestamp(i, v)
    this
  }

  override def setUUID(i: Int, v: UUID): Settable = {
    st.setUUID(i, v)
    this
  }

  override def setUDTValue(i: Int, v: UDTValue): Settable = {
    st.setUDTValue(i, v)
    this
  }

  override def setString(i: Int, v: String): Settable = {
    st.setString(i, v)
    this
  }

  override def setFloat(i: Int, v: Float): Settable = {
    st.setFloat(i, v)
    this
  }

  override def setInet(i: Int, v: InetAddress): Settable = {
    st.setInet(i, v)
    this
  }

  override def setList[E](i: Int, v: util.List[E]): Settable = {
    st.setList(i, v)
    this
  }

  override def setList[E](i: Int, v: util.List[E], elementsClass: Class[E]): Settable = {
    st.setList(i, v, elementsClass)
    this
  }

  override def setList[E](i: Int, v: util.List[E], elementsType: TypeToken[E]): Settable = {
    st.setList(i, v, elementsType)
    this
  }

  override def setTupleValue(i: Int, v: TupleValue): Settable = {
    st.setTupleValue(i, v)
    this
  }

  override def setDecimal(i: Int, v: BigDecimal): Settable = {
    st.setDecimal(i, v)
    this
  }

  override def setDouble(i: Int, v: Double): Settable = {
    st.setDouble(i, v)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V]): Settable = {
    st.setMap(i, v)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V], keysClass: Class[K], valuesClass: Class[V]): Settable = {
    st.setMap(i, v, keysClass, valuesClass)
    this
  }

  override def setMap[K, V](i: Int, v: util.Map[K, V], keysType: TypeToken[K], valuesType: TypeToken[V]): Settable = {
    st.setMap(i, v, keysType, valuesType)
    this
  }

  override def setToNull(i: Int): Settable = {
    st.setToNull(i)
    this
  }

  override def setTime(i: Int, v: Long): Settable = {
    st.setTime(i, v)
    this
  }

  override def setBytesUnsafe(i: Int, v: ByteBuffer): Settable = {
    st.setBytesUnsafe(i, v)
    this
  }

  override def setShort(i: Int, v: Short): Settable = {
    st.setShort(i, v)
    this
  }

  override def setBool(i: Int, v: Boolean): Settable = {
    st.setBool(i, v)
    this
  }

  override def setLong(i: Int, v: Long): Settable = {
    st.setLong(i, v)
    this
  }

  override def setVarint(i: Int, v: BigInteger): Settable = {
    st.setVarint(i, v)
    this
  }

  override def setInt(i: Int, v: Int): Settable = {
    st.setInt(i, v)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E]): Settable = {
    st.setSet(i, v)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E], elementsClass: Class[E]): Settable = {
    st.setSet(i, v, elementsClass)
    this
  }

  override def setSet[E](i: Int, v: util.Set[E], elementsType: TypeToken[E]): Settable = {
    st.setSet(i, v, elementsType)
    this
  }

  override def setByte(name: String, v: Byte): Settable = {
    st.setByte(name, v)
    this
  }

  override def setBytes(name: String, v: ByteBuffer): Settable = {
    st.setBytes(name, v)
    this
  }

  override def set[V](name: String, v: V, targetClass: Class[V]): Settable = {
    st.set(name, v, targetClass)
    this
  }

  override def set[V](name: String, v: V, targetType: TypeToken[V]): Settable = {
    st.set(name, v, targetType)
    this
  }

  override def set[V](name: String, v: V, codec: TypeCodec[V]): Settable = {
    st.set(name, v, codec)
    this
  }

  override def setDate(name: String, v: LocalDate): Settable = {
    st.setDate(name, v)
    this
  }

  override def setTimestamp(name: String, v: Date): Settable = {
    st.setTimestamp(name, v)
    this
  }

  override def setUUID(name: String, v: UUID): Settable = {
    st.setUUID(name, v)
    this
  }

  override def setUDTValue(name: String, v: UDTValue): Settable = {
    st.setUDTValue(name, v)
    this
  }

  override def setString(name: String, v: String): Settable = {
    st.setString(name, v)
    this
  }

  override def setFloat(name: String, v: Float): Settable = {
    st.setFloat(name, v)
    this
  }

  override def setInet(name: String, v: InetAddress): Settable = {
    st.setInet(name, v)
    this
  }

  override def setList[E](name: String, v: util.List[E]): Settable = {
    st.setList(name, v)
    this
  }

  override def setList[E](name: String, v: util.List[E], elementsClass: Class[E]): Settable = {
    st.setList(name, v, elementsClass)
    this
  }

  override def setList[E](name: String, v: util.List[E], elementsType: TypeToken[E]): Settable = {
    st.setList(name, v, elementsType)
    this
  }

  override def setTupleValue(name: String, v: TupleValue): Settable = {
    st.setTupleValue(name, v)
    this
  }

  override def setDecimal(name: String, v: BigDecimal): Settable = {
    st.setDecimal(name, v)
    this
  }

  override def setDouble(name: String, v: Double): Settable = {
    st.setDouble(name, v)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V]): Settable = {
    st.setMap(name, v)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V], keysClass: Class[K], valuesClass: Class[V]): Settable = {
    st.setMap(name, v, keysClass, valuesClass)
    this
  }

  override def setMap[K, V](name: String, v: util.Map[K, V], keysType: TypeToken[K], valuesType: TypeToken[V]): Settable = {
    st.setMap(name, v, keysType, valuesType)
    this
  }

  override def setToNull(name: String): Settable = {
    st.setToNull(name)
    this
  }

  override def setTime(name: String, v: Long): Settable = {
    st.setTime(name, v)
    this
  }

  override def setBytesUnsafe(name: String, v: ByteBuffer): Settable = {
    st.setBytesUnsafe(name, v)
    this
  }

  override def setShort(name: String, v: Short): Settable = {
    st.setShort(name, v)
    this
  }

  override def setBool(name: String, v: Boolean): Settable = {
    st.setBool(name, v)
    this
  }

  override def setLong(name: String, v: Long): Settable = {
    st.setLong(name, v)
    this
  }

  override def setVarint(name: String, v: BigInteger): Settable = {
    st.setVarint(name, v)
    this
  }

  override def setInt(name: String, v: Int): Settable = {
    st.setInt(name, v)
    this
  }

  override def setSet[E](name: String, v: util.Set[E]): Settable = {
    st.setSet(name, v)
    this
  }

  override def setSet[E](name: String, v: util.Set[E], elementsClass: Class[E]): Settable = {
    st.setSet(name, v, elementsClass)
    this
  }

  override def setSet[E](name: String, v: util.Set[E], elementsType: TypeToken[E]): Settable = {
    st.setSet(name, v, elementsType)
    this
  }

  def getDelegate[T]: T = st.asInstanceOf[T]

}
