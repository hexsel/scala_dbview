package com.xmatters.dbview

import java.io.InputStream
import java.sql.{Driver, DriverManager, ResultSet, Connection}
import java.util.Properties

abstract class Database {
  protected def typeMap: Map[String, String]

  def getGenericType(dbSpecificType: String): String = typeMap(dbSpecificType)

  def getSpecificType(genericType: String): String = {
    for (specificType <- typeMap.keys) {
      var mappedGenericType = typeMap(specificType)
      if (mappedGenericType.equalsIgnoreCase(genericType)) {
        return specificType
      }
    }
    null // not found
  }

  def isLengthlessType(sqlType : String): Boolean = false

  def minVarcharSize: Int = 1

  def getSchemaVersion(dbConnection: Connection): ResultSet

  def getColumnDefault(dbConnection: Connection, tableName: String, columnName: String): ResultSet

  def getCheckConstraints(dbConnection: Connection, tableName: String): ResultSet

  def getGenericDefault(dbSpecificDefault: String): String

  def getSpecificDefault(columnName: String, genericDefault: String): String

  def getProcedures(dbConnection: Connection): Map[String, String]

  def getSequences(dbConnection: Connection): ResultSet

  def getTriggers(dbConnection: Connection, tableName: String): ResultSet

  def getIndexColumns(dbConnection: Connection, schema: String, tableName: String): ResultSet

  def getProcedure(procedureName: String): String

  // TODO views, Unique constraints


  def generateSelectQuery(schema: String, table: Table): String = {
    val sb = new StringBuilder
    for (c <- table.columns) {
      if (sb.length > 0) {
        sb.append("SELECT ")
      }
      sb.append(c.name)
    }
    sb.append(" FROM ").append(schema).append(".").append(table.name)
    sb.toString()
  }

}

object Oracle extends Database {
  val ORACLE_GET_CHECK_CONSTRAINTS = "SELECT CONSTRAINT_NAME, SEARCH_CONDITION FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE='C' AND TABLE_NAME = ? AND NOT(CONSTRAINT_NAME LIKE 'SYS_%')"
  val ORACLE_GET_COLUMN_DEFAULT = "SELECT DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME=? AND COLUMN_NAME=?"
  val ORACLE_GET_PROCEDURES = "SELECT NAME, TEXT FROM USER_SOURCE WHERE TYPE='PROCEDURE' ORDER BY NAME, LINE"
  val ORACLE_GET_SEQUENCES = "SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER FROM USER_SEQUENCES order by SEQUENCE_NAME"
  val ORACLE_GET_TRIGGERS = "SELECT TRIGGER_NAME, TRIGGER_TYPE, TRIGGERING_EVENT, TRIGGER_BODY FROM USER_TRIGGERS WHERE TABLE_NAME = ?"
  val ORACLE_GET_VERSION = "SELECT MAX(ID) FROM DATABASECHANGELOG WHERE DATEEXECUTED = (SELECT MAX(DATEEXECUTED) FROM DATABASECHANGELOG)"
  val QUERY_INDEX_COLUMNS = "SELECT " +
      "  ind.INDEX_NAME, " +
      "  ind.INDEX_TYPE \"TYPE\", " +
      "  CASE ind.UNIQUENESS " +
      "    WHEN 'UNIQUE' THEN 'N' " +
      "    ELSE 'Y' " +
      "  END \"NON_UNIQUE\", " +
      "  col.COLUMN_NAME, " +
      "  col.DESCEND \"ASC_OR_DESC\" " +
      "  FROM USER_INDEXES ind, USER_IND_COLUMNS col " +
      " WHERE ind.TABLE_OWNER=? " +
      " AND col.TABLE_NAME =? " +
      " AND ind.index_name = col.index_name " +
      "ORDER BY col.COLUMN_POSITION";

  val typeMap = Map(
    "BLOB" -> "BLOB",
    "CHAR" -> "CHAR",
    "CLOB" -> "CLOB",
    "DATE" -> "TIMESTAMP",
    "NUMBER" -> "NUMERIC",
    "TIMESTAMP(6)" -> "TIMESTAMP",
    "TIMESTAMP(8)" -> "TIMESTAMP",
    "VARCHAR2" -> "VARCHAR"
  )

  def getCheckConstraints(dbConnection: Connection, tableName: String): ResultSet = {
    val stmt = dbConnection.prepareStatement(ORACLE_GET_CHECK_CONSTRAINTS)
    stmt.setString(1, tableName)
    stmt.executeQuery()
  }

  def getColumnDefault(dbConnection: Connection, tableName: String, columnName: String): ResultSet = {
    val ps = dbConnection.prepareStatement(ORACLE_GET_COLUMN_DEFAULT)
    ps.setString(1, tableName)
    ps.setString(2, columnName)
    ps.executeQuery()
  }

  def getGenericDefault(dbSpecificDefault: String): String = {
    if (dbSpecificDefault.equalsIgnoreCase("sys_extract_utc(systimestamp)")) {
      "CURRENT_UTC_TIME"
    } else if (dbSpecificDefault.toLowerCase.contains("to_date(to_char(sys_extract_utc")) {
      "CURRENT_UTC_TIME";
    } else {
      dbSpecificDefault
    }
  }

  def getProcedures(dbConnection: Connection): Map[String, String] = {
    var procedures = Map[String, String]()
    val ps = dbConnection.prepareStatement(ORACLE_GET_PROCEDURES)
    val rs = ps.executeQuery()
    while (rs.next()) {
      val procedureName = rs.getString(1);
      val bodyLine = rs.getString(2);
      var currentBody = procedures(procedureName);
      if (currentBody == null) {
        currentBody = "";
      }
      procedures = procedures.updated(procedureName, currentBody + bodyLine);
    }
    rs.close()
    ps.close()
    procedures
  }

  def getSchemaVersion(dbConnection: Connection): ResultSet = {
    val ps = dbConnection.prepareStatement(ORACLE_GET_VERSION)
    ps.executeQuery()
  }

  def getSequences(dbConnection: Connection): ResultSet = {
    val ps = dbConnection.prepareStatement(ORACLE_GET_SEQUENCES)
    ps.executeQuery()
  }

  def getSpecificDefault(columnName: String, genericDefault: String): String = {
    if ("CURRENT_UTC_TIME".equalsIgnoreCase("genericDefault")) {
      "to_date(to_char(sys_extract_utc(systimestamp),'yyyy mon dd HH24:MI:SS'),'yyyy mon dd HH24:MI:SS')";
    } else {
      genericDefault;
    }
  }

  def getTriggers(dbConnection: Connection, tableName: String): ResultSet = {
    val ps = dbConnection.prepareStatement(ORACLE_GET_TRIGGERS)
    ps.setString(1, tableName)
    ps.executeQuery()
  }

  def getIndexColumns(dbConnection: Connection, schema: String, tableName: String): ResultSet = {
    val stmt = dbConnection.prepareStatement(QUERY_INDEX_COLUMNS)
    stmt.setString(1, schema)
    stmt.setString(2, tableName)
    stmt.executeQuery()
  }

  def getProcedure(procedureName: String) = throw new RuntimeException("Not implemented yet"); // TODO
}


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


