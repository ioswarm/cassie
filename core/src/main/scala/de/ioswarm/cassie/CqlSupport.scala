package de.ioswarm.cassie

/**
  * Created by andreas on 20.08.17.
  */
trait CqlSupport {

  def cql: String

  override def toString: String = cql

}
