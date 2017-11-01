package de.ioswarm.cassie.demo

import java.util.UUID

import de.ioswarm.cassie._

/**
  * Created by andreas on 01.11.17.
  */
object Demo extends App {

  implicit val con = Cluster("test" simple 1)

  import Contact._

  val adr = Address("Teststrasse 15", "27000", "Teststadt", Country("DE", "Deutschland"))

  val c1 = Contact(UUID.randomUUID(), "Hannah", None, "Testmann", Some(adr))
  val c2 = Contact(UUID.randomUUID(), "Linus", None, "Testmann", Some(adr))
  val c3 = Contact(UUID.randomUUID(), "Milo", Some("Hans-Wurst"), "Testmann", None)

  table.insert(c1, c2, c3)

  sys exit 0

}
