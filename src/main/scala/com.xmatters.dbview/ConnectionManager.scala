package com.xmatters.dbview

import java.io.InputStream
import java.sql.{Driver, DriverManager, ResultSet, Connection}
import java.util.Properties

object ConnectionManager {
  val JDBC_DRIVER_CLASS_NAME = "JDBC_DRIVER_CLASS_NAME"
  val JDBC_URL = "JDBC_URL"
  val JDBC_USERNAME = "JDBC_USERNAME"
  val JDBC_PASSWORD = "JDBC_PASSWORD"

  def getConnection: Connection = {
    var connResource: InputStream = ConnectionManager.getClass
        .getClassLoader
        .getResourceAsStream(DumpDatabase.PROPERTIES_FILE)
    if (connResource == null) {
      System.err.println("Cannot load '" + DumpDatabase.PROPERTIES_FILE + "', please make sure it is in the same folder.")
      return null
    }
    var connProperties = new Properties
    connProperties.load(connResource)
    var driver: String = connProperties.getProperty(JDBC_DRIVER_CLASS_NAME)
    var url: String = connProperties.getProperty(JDBC_URL)
    var username: String = connProperties.getProperty(JDBC_USERNAME)
    var password: String = connProperties.getProperty(JDBC_PASSWORD)

    DriverManager.registerDriver(Class.forName(driver).newInstance.asInstanceOf[Driver])
    var connection: Connection = DriverManager.getConnection(url, username, password)

    connection.setAutoCommit(false)
    connection
  }
}





