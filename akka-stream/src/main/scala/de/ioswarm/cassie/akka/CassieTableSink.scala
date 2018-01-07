package de.ioswarm.cassie.akka

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import de.ioswarm.cassie.Cluster.Connection
import de.ioswarm.cassie.Table

import scala.concurrent.{ExecutionContext, Future}

object CassieTableSink {

  def apply[T](parallelism: Int = 100)(implicit connection: Connection, ex: ExecutionContext, tbl: Table[T]): Sink[T, Future[Done]] = Flow[T]
    .mapAsync(parallelism){t =>
      import de.ioswarm.cassie._

      TableStatement(None)(tbl).insertAsync(t)
    }
    .toMat(Sink.ignore)(Keep.right)

}
