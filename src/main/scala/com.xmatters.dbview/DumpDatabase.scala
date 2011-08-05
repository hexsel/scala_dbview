package com.xmatters.dbview

import org.slf4j.{LoggerFactory, Logger}
import java.nio.charset.Charset
import java.io._
import java.sql.{SQLException, Connection}
import runtime.Boolean

object DumpDatabase {
  var logger: Logger = LoggerFactory.getLogger(DumpDatabase.getClass)

  val PROPERTIES_FILE: String = "common.properties"

  def main(args: Array[String]) {
    var conn: Connection = null
    try {
      conn = ConnectionManager.getConnection
      if (conn == null) {
        System.err.println("Could not connect to the database. JDBC returned null with no exception.")
        return
      }
      logger.info("Connected to: " + conn.getMetaData.getURL + " as " + conn.getMetaData.getUserName)
      val schemaToUse = args(0)
      val tableFilter = args(1)
      val outputFilename = args(2)
      val isExportData = args(3).toBoolean
      var f = new File(outputFilename)
      var outStream = new FileOutputStream(f)
      var writer = new BufferedWriter(new OutputStreamWriter(outStream, Charset.forName("UTF-8")))
      var xmlizer = new XmlSerializer(
          Oracle, conn.getMetaData, schemaToUse, tableFilter, writer, isExportData)
      xmlizer.schemaToXML()

    } catch {
      case e: IOException => {
        System.err.println("Could not read connection properties from '" + PROPERTIES_FILE + "'")
        e.printStackTrace()
      }
      case e: ClassNotFoundException => {
        System.err.println("Could not find the database driver class")
        e.printStackTrace()
      }
      case e: SQLException => {
        System.err.println("Could not connect to the database using the settings in '" + PROPERTIES_FILE + "'")
        e.printStackTrace()
      }
      case e: InstantiationException => {
        System.err.println("Could not instantiate the database driver")
        e.printStackTrace()
      }
      case e: IllegalAccessException => {
        System.err.println("Could not instantiate the database driver")
        e.printStackTrace()
      }
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (conn != null) {
        conn.rollback()
        conn.close()
      }
    }
  }
}


