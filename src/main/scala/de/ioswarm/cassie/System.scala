package de.ioswarm.cassie

import java.nio.ByteBuffer
import java.util.UUID
import de.ioswarm.cassie.Cluster.Connection

/**
  * Created by andreas on 02.09.17.
  */
object System {

  implicit def keyspaceTable = Table("system_schema", "keyspaces", SysKeyspace.apply _, SysKeyspace.unapply _)(
    $"keyspace_name" partitionKey true
    , $"durable_writes"
    , $"replication"
  )

  case class SysKeyspace(keyspace: String
                      , durableWrites: Boolean
                      , replication: Map[String, String]
                     ) {

    def tables(implicit connection: Connection): Vector[SysTable] = System.tables.where($"keyspace_name" =:= keyspace).list(connection, System.systableTable)
    def columns(implicit connection: Connection): Vector[SysColumn] = System.columns.where($"keyspace_name" =:= keyspace).list(connection, System.syscolumnTable)
    def types(implicit connection: Connection): Vector[SysType] = System.types.where($"keyspace_name" =:= keyspace).list(connection, System.systypeTable)
    def indexes(implicit connection: Connection): Vector[SysIndex] = System.indexes.where($"keyspace_name" =:= keyspace).list(connection, System.sysindexTable)
  }

  def keyspaces: TypedSelectStatement[SysKeyspace] = from[SysKeyspace]("system_schema", "keyspaces")


  implicit def systableTable = Table("system_schema", "tables", SysTable.apply _, SysTable.unapply _)(
    $"keyspace_name" partitionKey true
    , $"table_name" clustering true clusteringOrder ASC
    , $"bloom_filter_fp_chance"
    , $"caching"
    , $"comment"
    , $"compaction"
    , $"compression"
    , $"crc_check_chance"
    , $"dclocal_read_repair_chance"
    , $"default_time_to_live"
    , $"extensions"
    , $"flags"
    , $"gc_grace_seconds"
    , $"id"
    , $"max_index_interval"
    , $"memtable_flush_period_in_ms"
    , $"min_index_interval"
    , $"read_repair_chance"
    , $"speculative_retry"
  )

  case class SysTable(keyspace: String
                   , name: String
                   , bloomFilter: Double
                   , caching: Map[String, String]
                   , comment: String
                   , compaction: Map[String, String]
                   , compression: Map[String, String]
                   , crcCheckChance: Double
                   , dclocalReadRepairChance: Double
                   , defaultTTL: Int
                   , extensions: Map[String, ByteBuffer]
                   , flags: Set[String]
                   , gcGraceSeconds: Int
                   , id: UUID
                   , maxIndexInterval: Int
                   , memtableFlushPeriod: Int
                   , minIndexInterval: Int
                   , readRepairChance: Double
                   , speculativeRetry: String
                   ) {
    def columns(implicit connection: Connection): Vector[SysColumn] = System.columns.where($"keyspace_name" =:= keyspace AND $"table_name" =:= name).list(connection, System.syscolumnTable)
    def indexes(implicit connection: Connection): Vector[SysIndex] = System.indexes.where($"keyspace_name" =:= keyspace AND $"table_name" =:= name).list(connection, System.sysindexTable)
  }

  def tables: TypedSelectStatement[SysTable] = from[SysTable]("system_schema", "tables")



  implicit def syscolumnTable = Table("system_schema", "columns", SysColumn.apply _, SysColumn.unapply _)(
    $"keyspace_name" partitionKey true
    , $"table_name" clustering true clusteringOrder ASC
    , $"column_name" clustering true clusteringOrder ASC
    , $"clustering_order"
    , $"column_name_bytes"
    , $"kind"
    , $"position"
    , $"type"
  )

  case class SysColumn(keyspace: String, tableName: String, name: String, clusteringOrder: String, columnNameBytes: ByteBuffer, kind: String, position: Int, dataType: String)

  def columns: TypedSelectStatement[SysColumn] = from[SysColumn]("system_schema", "columns")



  implicit def systypeTable = Table("system_schema", "types", SysType.apply _, SysType.unapply _)(
    $"keyspace_name" partitionKey true
    , $"type_name" clustering true clusteringOrder ASC
    , $"field_names"
    , $"field_types"
  )

  case class SysType(keyspace: String, name: String, fieldNames: List[String], fieldType: List[String])

  def types: TypedSelectStatement[SysType] = from[SysType]("system_schema", "types")


  implicit def sysindexTable = Table("system_schema", "indexes", SysIndex.apply _, SysIndex.unapply _)(
    $"keyspace_name" partitionKey true
    , $"table_name" clustering true clusteringOrder ASC
    , $"index_name" clustering true clusteringOrder ASC
    , $"kind"
    , $"options"
  )

  case class SysIndex(keyspace: String, tableName: String, name: String, kind: String, options: Map[String, String])

  def indexes: TypedSelectStatement[SysIndex] = from("system_schema", "indexes")
}
