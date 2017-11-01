package de.ioswarm.cassie

/**
  * Created by andreas on 29.08.17.
  */
sealed abstract class Order(value: String) {
  override def toString: String = value
}
case object NONE extends Order("")
case object ASC extends Order("ASC")
case object DESC extends Order("DESC")