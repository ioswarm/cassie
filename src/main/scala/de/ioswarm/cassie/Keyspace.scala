package de.ioswarm.cassie

/**
  * Created by andreas on 26.09.17.
  */
case class Keyspace(keyspace: String = ""
                    , durableWrites: Boolean = true
                    , replication: Map[String, String] = Map(
  "class" -> "SimpleStrategy"
  , "replication_factor" -> "3")) {

  private val sys_keyspaces = Seq("system", "system_auth", "system_distributed", "system_schema", "system_traces", "")

  def durableWrites(b: Boolean): Keyspace = copy(durableWrites = b)

  def simpleStrategy(replicationFactor: Int): Keyspace = copy(replication = Map(
    "class" -> "SimpleStrategy"
    , "replication_factor" -> replicationFactor.toString
  ))

  def simple(replicationFactor: Int): Keyspace = simpleStrategy(replicationFactor)

  def networkTopologyStrategy(dataCenter: (String, Int)*): Keyspace = copy(
    replication = dataCenter.map(t => t._1 -> t._2.toString).toMap + ("class" -> "NetworkTopologyStrategy")
  )
  def network(dataCenter: (String, Int)*): Keyspace = networkTopologyStrategy(dataCenter :_*)

  def cqlCreate: String = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {%s} %s".format(
    keyspace
    , replication.map(t => "'"+t._1+"':'"+t._2+"'").mkString(", ")
    , if (!durableWrites) "AND DURABLE_WRITES = false" else ""
  )

  def cqlDrop: String = s"DROP KEYSPACE IF EXISTS $keyspace"

  private def execute(cql: String): Unit = {
    Cluster().execute(cql)
  }

  def create(): Unit = if (!sys_keyspaces.contains(keyspace.toLowerCase)) execute(cqlCreate)

  def drop(): Unit = if (!sys_keyspaces.contains(keyspace.toLowerCase)) execute(cqlDrop)

  def isRoot: Boolean = keyspace.isEmpty

}
