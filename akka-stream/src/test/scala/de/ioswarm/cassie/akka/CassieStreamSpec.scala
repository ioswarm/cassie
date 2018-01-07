package de.ioswarm.cassie.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import de.ioswarm.cassie.Cluster.Connection
import de.ioswarm.cassie._
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

object CassieStreamSpec {

  implicit def tablePerson(implicit con: Connection): Table[Person] = Table("person", Person.apply _, Person.unapply _)(
    "name" pk()
    , "age"
  ).create

  case class Person(name: String, age: Int)
}
class CassieStreamSpec extends WordSpec with ScalaFutures with BeforeAndAfterAll with MustMatchers {

  import CassieStreamSpec._

  implicit val system: ActorSystem = ActorSystem("cassie-stream-spec")
  implicit val mat: ActorMaterializer = ActorMaterializer()

  val keyspace = Keyspace("test_"+java.lang.System.nanoTime())

  override def afterAll(): Unit = {
    keyspace.drop()
    Cluster.close()
    Await.result(system.terminate(), 5.seconds)
  }

  "CassieStreamSpec" must {
    import system.dispatcher

    implicit val con: Connection = Cluster(keyspace)

    val data = List(
      Person("Hannah", 18)
      , Person("Linus", 19)
      , Person("Milo", 15)
    )


    "write data to table 'PERSON' via CassieSink" in {
      val res = Source(data).runWith(CassieTableSink())

      res.futureValue

      table[Person].fetch.toSet mustBe data.toSet
    }
  }
}
