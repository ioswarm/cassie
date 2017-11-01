package de.ioswarm.cassie

import java.util.UUID

/**
  * Created by andreas on 14.09.17.
  */
object Contact {
  implicit val tblContact = Table("test", "contact", Contact.apply _, Contact.unapply _)(
    $"id" partitionKey true
    , $"firstname"
    , $"secondname"
    , $"surname" index "surname"
    , $"address"
  ).create
}
case class Contact(id: UUID, firstName: String, secondName: Option[String], surname: String, address: Option[Address]) {

}
