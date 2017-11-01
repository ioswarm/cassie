package de.ioswarm.cassie

/**
  * Created by andreas on 14.09.17.
  */
object Address{

  import Country._

  implicit val typeAddress = Type("test", "address", Address.apply _, Address.unapply _)(
    $"street"
    , $"postalCode"
    , $"city"
    , $"country"
  )
}
case class Address(street: String, postalCode: String, city: String, country: Country) {

}
