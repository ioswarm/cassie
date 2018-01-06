package de.ioswarm.cassie

/**
  * Created by andreas on 27.08.17.
  */
trait Reader[T] {
  def read(index: Int, stmt: Gettable): T
  def read(name: String, stmt: Gettable): T
}
