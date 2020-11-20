// Databricks notebook source
// DBTITLE 1,Bibliotecas y variables
import java.io.InputStream
import java.sql.DriverManager
import java.util.Properties
import java.util.Properties
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{DataFrame, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.apache.spark.sql.SaveMode

//Parametros de la prueba
val particiones = "12000"
val maxBatchSize = "100000"

val driverClass = "org.postgresql.Driver"

// Create the JDBC URL without passing in the user and password parameters.

val jdbcUrl = "jdbc:postgresql://¯|_(ツ)_|¯c.postgres.database.azure.com:5432/citus?user=¯|_(ツ)_|¯&password=¯|_(ツ)_|¯&sslmode=require"

// Create a Properties() object to hold the parameters.

val connectionProperties = new Properties()
connectionProperties.setProperty("Driver", driverClass)
connectionProperties.setProperty("batchsize", maxBatchSize)

val path_bronce = "dbfs:/mnt/lake/bronce/"
val path_plata = "dbfs:/mnt/lake/plata/"
val path_tabla_delta = path_plata + "concepto/"

sqlContext.setConf("spark.sql.shuffle.partitions", particiones)
sqlContext.setConf("spark.default.parallelism", particiones)

// DBTITLE 1,Tabla delta fuente
//Leyendo datos fuentes en formato parquet
val parquetFileDF = spark.read.format("delta").load(path_tabla_delta) 
parquetFileDF.count()

parquetFileDF.rdd.getNumPartitions

// DBTITLE 1,Prueba de escritura con jdbc ( multi row inserts)
parquetFileDF.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "db.table", connectionProperties)

// DBTITLE 1,Clase que permite la ingestión de datos utilizando CopyManager
object CopyHelper extends Serializable {

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.map { row =>
      (row.toSeq
        .map { v =>
          if (v == null) {
            """\N"""
          } else {
            "\"" + v.toString.replaceAll("\"", "\"\"") + "\""
          }
        }
        .mkString("\t") + "\n").getBytes
    }.flatten

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) {
          bytes.next & 0xff // make the signed byte an unsigned int
        } else {
          -1
        }
    }
  }

  def copyIn(url: String, df: DataFrame, table: String):Unit = {
    var cols = df.columns.mkString(",")

    df.rdd.foreachPartition { rows =>
      val conn = DriverManager.getConnection(url)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        cm.copyIn(
          s"COPY $table ($cols) " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')""",
          rowsToInputStream(rows))
        ()
      } finally {
        conn.close()
      }
    }
  }
}

// DBTITLE 1,Prueba de escritura con Copy
CopyHelper.copyIn(jdbcUrl, parquetFileDF, "db.table")