package de.ioswarm.cassie

import java.net.InetAddress
import java.nio.ByteBuffer
import java.sql.{Time, Timestamp, Date => SQLDate}
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId}
import java.util.{Date, UUID}

import com.datastax.driver.core.LocalDate

/**
  * Created by andreas on 24.08.17.
  */
trait CassandraBasicFormats {

  implicit def uuidFormat = new CassandraBasicFormat[UUID]("uuid") {
    override def read(index: Int, stmt: Gettable): UUID = stmt.getUUID(index)

    override def read(name: String, stmt: Gettable): UUID = stmt.getUUID(name)

    override def write(name: String, t: UUID, stmt: Settable): Settable = stmt.setUUID(name, t)

    override def write(index: Int, t: UUID, stmt: Settable): Settable = stmt.setUUID(index, t)
  }

  implicit def timestampFormat = new CassandraBasicFormat[Timestamp]("timestamp") {
    override def toCql(t: Timestamp): String = new SimpleDateFormat("''yyyy-MM-dd'T'HH:mm:ss.SSSZ''").format(t)

    override def read(index: Int, stmt: Gettable): Timestamp = new Timestamp(stmt.getTimestamp(index).getTime)

    override def read(name: String, stmt: Gettable): Timestamp = new Timestamp(stmt.getTimestamp(name).getTime)

    override def write(name: String, t: Timestamp, stmt: Settable): Settable = stmt.setTimestamp(name, new Date(t.getTime))

    override def write(index: Int, t: Timestamp, stmt: Settable): Settable = stmt.setTimestamp(index, new Date(t.getTime))
  }

  implicit def instantFormat = new CassandraBasicFormat[Instant]("timestamp") {
    override def read(index: Int, stmt: Gettable): Instant = Instant.ofEpochMilli(stmt.getTimestamp(index).getTime)

    override def read(name: String, stmt: Gettable): Instant = Instant.ofEpochMilli(stmt.getTimestamp(name).getTime)

    override def write(name: String, t: Instant, stmt: Settable): Settable = stmt.setTimestamp(name, Date.from(t))

    override def write(index: Int, t: Instant, stmt: Settable): Settable = stmt.setTimestamp(index, Date.from(t))
  }

  implicit def varintFormat = new CassandraBasicFormat[BigInt]("varint") {
    override def read(index: Int, stmt: Gettable): BigInt = stmt.getVarint(index)

    override def read(name: String, stmt: Gettable): BigInt = stmt.getVarint(name)

    override def write(name: String, t: BigInt, stmt: Settable): Settable = stmt.setVarint(name, t.bigInteger)

    override def write(index: Int, t: BigInt, stmt: Settable): Settable = stmt.setVarint(index, t.bigInteger)
  }

  implicit def inetaddrFormat = new CassandraBasicFormat[InetAddress]("inet") {
    override def toCql(t: InetAddress): String = "'"+t.getHostName+"'"

    override def read(index: Int, stmt: Gettable): InetAddress = stmt.getInet(index)

    override def read(name: String, stmt: Gettable): InetAddress = stmt.getInet(name)

    override def write(name: String, t: InetAddress, stmt: Settable): Settable = stmt.setInet(name, t)

    override def write(index: Int, t: InetAddress, stmt: Settable): Settable = stmt.setInet(index, t)
  }

  implicit def bytearrayFormat = new CassandraBasicFormat[Array[Byte]]("blob") {
    override def toCql(t: Array[Byte]): String = t.toHex

    def byteBufferToByteArray(buf: ByteBuffer): Array[Byte] = {
      buf.clear()
      val bytes = new Array[Byte](buf.capacity())
      buf.get(bytes, 0, bytes.length)
      bytes
    }

    override def read(index: Int, stmt: Gettable): Array[Byte] = byteBufferToByteArray(stmt.getBytes(index))

    override def read(name: String, stmt: Gettable): Array[Byte] = byteBufferToByteArray(stmt.getBytes(name))

    override def write(name: String, t: Array[Byte], stmt: Settable): Settable = stmt.setBytes(name, ByteBuffer.wrap(t))

    override def write(index: Int, t: Array[Byte], stmt: Settable): Settable = stmt.setBytes(index, ByteBuffer.wrap(t))
  }

  implicit def bytebufferFormat = new CassandraBasicFormat[ByteBuffer]("blob") {
    override def toCql(t: ByteBuffer): String = t.toHex

    override def read(index: Int, stmt: Gettable): ByteBuffer = stmt.getBytes(index)

    override def read(name: String, stmt: Gettable): ByteBuffer = stmt.getBytes(name)

    override def write(name: String, t: ByteBuffer, stmt: Settable): Settable = stmt.setBytes(name, t)

    override def write(index: Int, t: ByteBuffer, stmt: Settable): Settable = stmt.setBytes(index, t)
  }

  implicit def floatFormat = new CassandraBasicFormat[Float]("float") {
    override def read(index: Int, stmt: Gettable): Float = stmt.getFloat(index)

    override def read(name: String, stmt: Gettable): Float = stmt.getFloat(name)

    override def write(name: String, t: Float, stmt: Settable): Settable = stmt.setFloat(name, t)

    override def write(index: Int, t: Float, stmt: Settable): Settable = stmt.setFloat(index, t)
  }

  implicit def longFormat = new CassandraBasicFormat[Long]("bigint") {
    override def read(index: Int, stmt: Gettable): Long = stmt.getLong(index)

    override def read(name: String, stmt: Gettable): Long = stmt.getLong(name)

    override def write(name: String, t: Long, stmt: Settable): Settable = stmt.setLong(name, t)

    override def write(index: Int, t: Long, stmt: Settable): Settable = stmt.setLong(index, t)
  }

  implicit def booleanFormat = new CassandraBasicFormat[Boolean]("boolean") {
    override def toCql(t: Boolean): String = if (t) "true" else "false"

    override def read(index: Int, stmt: Gettable): Boolean = stmt.getBool(index)

    override def read(name: String, stmt: Gettable): Boolean = stmt.getBool(name)

    override def write(name: String, t: Boolean, stmt: Settable): Settable = stmt.setBool(name, t)

    override def write(index: Int, t: Boolean, stmt: Settable): Settable = stmt.setBool(index, t)
  }

  implicit def timeFormat = new CassandraBasicFormat[Time]("time") {
    override def toCql(t: Time): String = new SimpleDateFormat("HH:mm:ss.SSS").format(t)

    override def read(index: Int, stmt: Gettable): Time = new Time(stmt.getTime(index))

    override def read(name: String, stmt: Gettable): Time = new Time(stmt.getTime(name))

    override def write(name: String, t: Time, stmt: Settable): Settable = stmt.setTime(name, t.getTime)

    override def write(index: Int, t: Time, stmt: Settable): Settable = stmt.setTime(index, t.getTime)
  }

  implicit def byteFormat = new CassandraBasicFormat[Byte]("tinyint") {
    override def read(index: Int, stmt: Gettable): Byte = stmt.getByte(index)

    override def read(name: String, stmt: Gettable): Byte = stmt.getByte(name)

    override def write(name: String, t: Byte, stmt: Settable): Settable = stmt.setByte(name, t)

    override def write(index: Int, t: Byte, stmt: Settable): Settable = stmt.setByte(index, t)
  }

  implicit def decimalFormat = new CassandraBasicFormat[BigDecimal]("decimal") {
    override def read(index: Int, stmt: Gettable): BigDecimal = stmt.getDecimal(index)

    override def read(name: String, stmt: Gettable): BigDecimal = stmt.getDecimal(name)

    override def write(name: String, t: BigDecimal, stmt: Settable): Settable = stmt.setDecimal(name, t.bigDecimal)

    override def write(index: Int, t: BigDecimal, stmt: Settable): Settable = stmt.setDecimal(index, t.bigDecimal)
  }

  implicit def shortFormat = new CassandraBasicFormat[Short]("smallint") {
    override def read(index: Int, stmt: Gettable): Short = stmt.getShort(index)

    override def read(name: String, stmt: Gettable): Short = stmt.getShort(name)

    override def write(name: String, t: Short, stmt: Settable): Settable = stmt.setShort(name, t)

    override def write(index: Int, t: Short, stmt: Settable): Settable = stmt.setShort(index ,t)
  }

  implicit def dateFormat = new CassandraBasicFormat[Date]("date") {
    override def toCql(t: Date): String = new SimpleDateFormat("yyyy-MM-dd").format(t)

    override def read(index: Int, stmt: Gettable): Date = new Date(stmt.getDate(index).getMillisSinceEpoch)

    override def read(name: String, stmt: Gettable): Date = new Date(stmt.getDate(name).getMillisSinceEpoch)

    override def write(name: String, t: Date, stmt: Settable): Settable = stmt.setDate(name, LocalDate.fromMillisSinceEpoch(t.getTime))

    override def write(index: Int, t: Date, stmt: Settable): Settable = stmt.setDate(index, LocalDate.fromMillisSinceEpoch(t.getTime))
  }

  implicit def sqldateformat = new CassandraBasicFormat[SQLDate]("date") {
    override def toCql(t: SQLDate): String = new SimpleDateFormat("yyyy-MM-dd").format(t)

    override def read(index: Int, stmt: Gettable): SQLDate = new SQLDate(stmt.getDate(index).getMillisSinceEpoch)

    override def read(name: String, stmt: Gettable): SQLDate = new SQLDate(stmt.getDate(name).getMillisSinceEpoch)

    override def write(name: String, t: SQLDate, stmt: Settable): Settable = stmt.setDate(name, LocalDate.fromMillisSinceEpoch(t.getTime))

    override def write(index: Int, t: SQLDate, stmt: Settable): Settable = stmt.setDate(index, LocalDate.fromMillisSinceEpoch(t.getTime))
  }

  implicit def doubleFormat = new CassandraBasicFormat[Double]("double") {
    override def read(index: Int, stmt: Gettable): Double = stmt.getDouble(index)

    override def read(name: String, stmt: Gettable): Double = stmt.getDouble(name)

    override def write(name: String, t: Double, stmt: Settable): Settable = stmt.setDouble(name, t)

    override def write(index: Int, t: Double, stmt: Settable): Settable = stmt.setDouble(index, t)
  }

  implicit def stringFormat = new CassandraBasicFormat[String]("text") {
    override def toCql(t: String): String = "'"+t+"'"

    override def read(index: Int, stmt: Gettable): String = stmt.getString(index)

    override def read(name: String, stmt: Gettable): String = stmt.getString(name)

    override def write(name: String, t: String, stmt: Settable): Settable = stmt.setString(name, t)

    override def write(index: Int, t: String, stmt: Settable): Settable = stmt.setString(index, t)
  }

  implicit def intFormat = new CassandraBasicFormat[Int]("int") {
    override def read(index: Int, stmt: Gettable): Int = stmt.getInt(index)

    override def read(name: String, stmt: Gettable): Int = stmt.getInt(name)

    override def write(name: String, t: Int, stmt: Settable): Settable = stmt.setInt(name, t)

    override def write(index: Int, t: Int, stmt: Settable): Settable = stmt.setInt(index, t)
  }


}
