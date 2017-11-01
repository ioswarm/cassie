package de.ioswarm.cassie

import de.ioswarm.cassie.Cluster.Connection

/**
  * Created by andreas on 14.09.17.
  */
object Country {
  implicit val tblCountry = Type("test", "country", Country.apply _, Country.unapply _)(
    $"iso"
    , $"name"
  )
}
case class Country(iso: String, name: String) {

}
