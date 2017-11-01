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
case class Gettable(gt: GettableData) extends GettableData {

  override def getUUID(name: String): UUID = gt.getUUID(name)

  override def getTimestamp(name: String): Date = gt.getTimestamp(name)

  override def getVarint(name: String): BigInteger = gt.getVarint(name)

  override def getTupleValue(name: String): TupleValue = gt.getTupleValue(name)

  override def getInet(name: String): InetAddress = gt.getInet(name)

  override def get[T](name: String, targetClass: Class[T]): T = gt.get(name, targetClass)

  override def get[T](name: String, targetType: TypeToken[T]): T = gt.get(name, targetType)

  override def get[T](name: String, codec: TypeCodec[T]): T = gt.get(name, codec)

  override def getList[T](name: String, elementsClass: Class[T]): util.List[T] = gt.getList(name, elementsClass)

  override def getList[T](name: String, elementsType: TypeToken[T]): util.List[T] = gt.getList(name, elementsType)

  override def getDouble(name: String): Double = gt.getDouble(name)

  override def getBytesUnsafe(name: String): ByteBuffer = gt.getBytesUnsafe(name)

  override def getUDTValue(name: String): UDTValue = gt.getUDTValue(name)

  override def getFloat(name: String): Float = gt.getFloat(name)

  override def getLong(name: String): Long = gt.getLong(name)

  override def getBool(name: String): Boolean = gt.getBool(name)

  override def getMap[K, V](name: String, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = gt.getMap(name, keysClass, valuesClass)

  override def getMap[K, V](name: String, keysType: TypeToken[K], valuesType: TypeToken[V]): util.Map[K, V] = gt.getMap(name, keysType, valuesType)

  override def getTime(name: String): Long = gt.getTime(name)

  override def getByte(name: String): Byte = gt.getByte(name)

  override def getDecimal(name: String): BigDecimal = gt.getDecimal(name)

  override def getObject(name: String): AnyRef = gt.getObject(name)

  override def getShort(name: String): Short = gt.getShort(name)

  override def isNull(name: String): Boolean = gt.isNull(name)

  override def getSet[T](name: String, elementsClass: Class[T]): util.Set[T] = gt.getSet(name, elementsClass)

  override def getSet[T](name: String, elementsType: TypeToken[T]): util.Set[T] = gt.getSet(name, elementsType)

  override def getDate(name: String): LocalDate = gt.getDate(name)

  override def getInt(name: String): Int = gt.getInt(name)

  override def getBytes(name: String): ByteBuffer = gt.getBytes(name)

  override def getString(name: String): String = gt.getString(name)

  override def getUUID(i: Int): UUID = gt.getUUID(i)

  override def getTimestamp(i: Int): Date = gt.getTimestamp(i)

  override def getVarint(i: Int): BigInteger = gt.getVarint(i)

  override def getTupleValue(i: Int): TupleValue = gt.getTupleValue(i)

  override def getInet(i: Int): InetAddress = gt.getInet(i)

  override def get[T](i: Int, targetClass: Class[T]): T = gt.get(i, targetClass)

  override def get[T](i: Int, targetType: TypeToken[T]): T = gt.get(i, targetType)

  override def get[T](i: Int, codec: TypeCodec[T]): T = gt.get(i, codec)

  override def getList[T](i: Int, elementsClass: Class[T]): util.List[T] = gt.getList(i, elementsClass)

  override def getList[T](i: Int, elementsType: TypeToken[T]): util.List[T] = gt.getList(i, elementsType)

  override def getDouble(i: Int): Double = gt.getDouble(i)

  override def getBytesUnsafe(i: Int): ByteBuffer = gt.getBytesUnsafe(i)

  override def getUDTValue(i: Int): UDTValue = gt.getUDTValue(i)

  override def getFloat(i: Int): Float = gt.getFloat(i)

  override def getLong(i: Int): Long = gt.getLong(i)

  override def getBool(i: Int): Boolean = gt.getBool(i)

  override def getMap[K, V](i: Int, keysClass: Class[K], valuesClass: Class[V]): util.Map[K, V] = gt.getMap(i, keysClass, valuesClass)

  override def getMap[K, V](i: Int, keysType: TypeToken[K], valuesType: TypeToken[V]): util.Map[K, V] = gt.getMap(i, keysType, valuesType)

  override def getTime(i: Int): Long = gt.getTime(i)

  override def getByte(i: Int): Byte = gt.getByte(i)

  override def getDecimal(i: Int): BigDecimal = gt.getDecimal(i)

  override def getObject(i: Int): AnyRef = gt.getObject(i)

  override def getShort(i: Int): Short = gt.getShort(i)

  override def isNull(i: Int): Boolean = gt.isNull(i)

  override def getSet[T](i: Int, elementsClass: Class[T]): util.Set[T] = gt.getSet(i, elementsClass)

  override def getSet[T](i: Int, elementsType: TypeToken[T]): util.Set[T] = gt.getSet(i, elementsType)

  override def getDate(i: Int): LocalDate = gt.getDate(i)

  override def getInt(i: Int): Int = gt.getInt(i)

  override def getBytes(i: Int): ByteBuffer = gt.getBytes(i)

  override def getString(i: Int): String = gt.getString(i)

  def getDelegate[T]: T = gt.asInstanceOf[T]

}
