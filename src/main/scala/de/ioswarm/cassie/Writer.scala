package de.ioswarm.cassie

/**
  * Created by andreas on 27.08.17.
  */
trait Writer[T] {
  def write(name: String, t: T, stmt: Settable): Settable
  def write(index: Int, t: T, stmt: Settable): Settable
}
