package com.xmatters.dbview

import org.slf4j.LoggerFactory
import javax.xml.stream.XMLOutputFactory
import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import java.io._
import java.util.TimeZone
import java.sql._
import java.text.SimpleDateFormat

/**
 * Class to convert DatabaseMetaData information into XML format
 *
 * @author jleung
 */
class XmlSerializer(
    utility: Database,
    dbMetaData: DatabaseMetaData,
    schema: String,
    tableFilter: String,
    writer: Writer,
    exportData: Boolean)
{
  private val RECORDS_TO_FETCH: Int = 1000

  private val df: SimpleDateFormat = new SimpleDateFormat
  df.setTimeZone(TimeZone.getTimeZone("GMT"))
  df.applyPattern("yyyy/MM/dd HH:mm:ss.SSS")

  val LOGGER = LoggerFactory.getLogger(classOf[XmlSerializer])
  val SCHEMA_TOKEN = "ape-schema"
  val SCHEMA_VERSION_TOKEN = "version"
  val SCHEMA_TABLE_FILTER_TOKEN = "table-filter"
  val REMARKS = "REMARKS"
  val TABLE_NAME_RS_COL = "TABLE_NAME"
  val TABLE_TOKEN = "table"
  val TABLE_NAME_TOKEN = "name"
  val TABLE_COMMENT_TOKEN = "comment"
  val COLUMN_NAME_RS_COL = "COLUMN_NAME"
  val COLUMN_TYPE_RS_COL = "TYPE_NAME"
  val COLUMN_SIZE_RS_COL = "COLUMN_SIZE"
  val COLUMN_NULLABLE_RS_COL = "IS_NULLABLE"
  val COLUMN_DEFAULT_RS_COL = "COLUMN_DEF"
  val COLUMN_REMARKS_RS_COL = "REMARKS"
  val COLUMN_TOKEN = "column"
  val COLUMN_NAME_TOKEN = "name"
  val COLUMN_TYPE_TOKEN = "type"
  val COLUMN_LENGTH_TOKEN = "length"
  val COLUMN_REQUIRE_TOKEN = "require"
  val COLUMN_COMMENT_TOKEN = "comment"
  val COLUMN_DEFAULT_TOKEN = "default"
  val INDEX_NAME_RS_COL = "INDEX_NAME"
  val INDEX_TYPE_RS_COL = "TYPE"
  val INDEX_COLUMN_RS_COL = "COLUMN_NAME"
  val INDEX_UNIQUE_RS_COL = "UNIQUE"
  val INDEX_DIRECTION_RS_COL = "ASC_OR_DESC"
  val INDEX_TOKEN = "index"
  val INDEX_NAME_TOKEN = "name"
  val INDEX_UNIQUE_TOKEN = "unique"
  val INDEX_COLUMN_TOKEN = "column"
  val INDEX_COLUMN_NAME_TOKEN = "name"
  val INDEX_DECENDING_TOKEN = "column"
  val PK_NAME_RS_COL = "PK_NAME"
  val PK_COLUMN_NAME_RS_COL = "COLUMN_NAME"
  val PRIMARY_KEY_TOKEN = "primaryKey"
  val PRIMARY_KEY_NAME_TOKEN = "name"
  val REFERENCE_TOKEN = "reference"
  val LOCAL_COLUMN_TOKEN = "localColumn"
  val FK_NAME_RS_COL = "FK_NAME"
  val FK_DELETE_RULE_RS_COL = "DELETE_RULE"
  val FK_COLUMN_SEQ_RS_COL = "KEY_SEQ"
  val FK_REFERENCED_TABLE_RS_COL = "PKTABLE_NAME"
  val FK_COLUMN_NAME_RS_COL = "FKCOLUMN_NAME"
  val FK_REFERENCED_COLUMN_NAME_RS_COL = "PKCOLUMN_NAME"
  val FK_TOKEN = "foreignKey"
  val FK_NAME_TOKEN = "name"
  val FK_REFERENCED_TABLE_TOKEN = "foreignTable"
  val FK_ON_DELETE_TOKEN = "onDelete"
  val FK_REFERENCE_TOKEN = "reference"
  val FK_LOCAL_COLUMN_TOKEN = "localColumn"
  val FK_REFERENCED_COLUMN_TOKEN = "foreignColumn"
  val CHK_NAME_RS_COL = "CONSTRAINT_NAME"
  val CHK_CONDITION_RS_COL = "SEARCH_CONDITION"
  val CHK_TOKEN = "checkConstraint"
  val CHK_NAME_TOKEN = "name"
  val CHK_CLAUSE_TOKEN = "checkClause"
  val TRIGGER_TOKEN = "trigger"
  val TRIGGER_NAME_TOKEN = "name"
  val TRIGGER_TYPE_TOKEN = "type"
  val TRIGGER_EVENT_TOKEN = "event"
  val PROCEDURE_TOKEN = "procedure"
  val PROCEDURE_NAME_TOKEN = "name"
  val PROCEDURE_BODY_TOKEN = "body"
  val SEQUENCE_TOKEN = "sequence"
  val SEQUENCE_NAME_TOKEN = "name"
  val SEQUENCE_MIN_TOKEN = "min"
  val SEQUENCE_MAX_TOKEN = "max"
  val SEQUENCE_INCREMENT_BY_TOKEN = "increment"
  val SEQUENCE_LAST_NUMBER_TOKEN = "lastNumber"
  val ROW_TOKEN = "r"
  val VALUE_TOKEN = "v"
  val VALUE_INDEX_TOKEN = "i"

  val xmlWriter = new IndentingXMLStreamWriter(XMLOutputFactory.newInstance.createXMLStreamWriter(writer))

  /**
   * Build an XML representation of a column and writes it to the local XML
   * writer that was passed in from the constructor.
   */
  private def columnToXML(column: Column) {
    xmlWriter.writeEmptyElement(COLUMN_TOKEN)
    xmlWriter.writeAttribute(COLUMN_NAME_TOKEN, column.name.toLowerCase)
    xmlWriter.writeAttribute(COLUMN_TYPE_TOKEN, column.sqlType)
    xmlWriter.writeAttribute(COLUMN_LENGTH_TOKEN, Integer.toString(column.size))
    xmlWriter.writeAttribute(COLUMN_REQUIRE_TOKEN, (!column.nullable).toString)

    val defaultValue = column.defaultValue
    if (defaultValue != null && defaultValue.length > 0) {
      xmlWriter.writeAttribute(COLUMN_DEFAULT_TOKEN, utility.getGenericDefault(defaultValue.trim))
    }

    val remarks = column.comments
    if (remarks != null && remarks.length > 0) {
      xmlWriter.writeAttribute(COLUMN_COMMENT_TOKEN, remarks)
    }
  }

  private def doCheckConstraints(tableName: String) {
    val numOfCheckConstraints = 0
    val conn = dbMetaData.getConnection
    val checkConstraintsRS = utility.getCheckConstraints(conn, tableName)
    while (checkConstraintsRS.next) {
      numOfCheckConstraints += 1
      val name = checkConstraintsRS.getString(1)
      val clause = checkConstraintsRS.getString(2)
      xmlWriter.writeStartElement(CHK_TOKEN)
      xmlWriter.writeAttribute(CHK_NAME_TOKEN, name)
      xmlWriter.writeStartElement(CHK_CLAUSE_TOKEN)
      xmlWriter.writeCData(clause)
      xmlWriter.writeEndElement()
      xmlWriter.writeEndElement()
    }
    checkConstraintsRS.getStatement.close()
    LOGGER.info("  Check Constraints: " + numOfCheckConstraints)
  }

  private def doColumns(tableName: String): List[Column] = {
    var columns = List[Column]()
    val columnsRS = dbMetaData.getColumns(null, schema, tableName, "%")
    while (columnsRS.next) {
      var defaultValue = ""
      val name = columnsRS.getString(COLUMN_NAME_RS_COL)
      val sqlType = utility.getGenericType(columnsRS.getString(COLUMN_TYPE_RS_COL))
      val size = columnsRS.getInt(COLUMN_SIZE_RS_COL)
      val nullable = columnsRS.getString(COLUMN_NULLABLE_RS_COL).equalsIgnoreCase("NO")
      try {
        defaultValue = columnsRS.getString(COLUMN_DEFAULT_RS_COL)
      } catch {
        case ex: SQLException => defaultValue = getColumnDefault(tableName, name)
      }
      val columnRemarks = columnsRS.getString(COLUMN_REMARKS_RS_COL)
      val column = new Column(name, sqlType, size, nullable, defaultValue, columnRemarks)
      columnToXML(column)
      columns = column :: columns
    }
    columnsRS.getStatement.close()
    LOGGER.info("  Columns: " + columns.size)
    columns
  }

  private def doForeignKeys(tableName: String): List[ForeignKey] = {
    var fkRS: ResultSet = null
    try {
      fkRS = dbMetaData.getImportedKeys(null, schema, tableName)
      var foreignKeys = Map[String, ForeignKey]()
      while (fkRS.next) {
        val name = fkRS.getString(FK_NAME_RS_COL)
        var fk = foreignKeys(name)
        if (fk == null) {
          val deleteRule = fkRS.getShort(FK_DELETE_RULE_RS_COL)
          val referencedTable = fkRS.getString(FK_REFERENCED_TABLE_RS_COL)
          val deleteRuleName = deleteRule match {
            case DatabaseMetaData.importedKeyRestrict => "NO ACTION"
            case DatabaseMetaData.importedKeyNoAction => "NO ACTION"
            case DatabaseMetaData.importedKeyCascade => "CASCADE"
            case DatabaseMetaData.importedKeySetNull => "SET NULL"
            case DatabaseMetaData.importedKeySetDefault => "SET DEFAULT"
            case _ => ""
          }
          fk = new ForeignKey(name, referencedTable, fkColumns, pkColumns, deleteRuleName)
        }
        val pkColumn = fkRS.getString(FK_REFERENCED_COLUMN_NAME_RS_COL)
        val fkColumn = fkRS.getString(FK_COLUMN_NAME_RS_COL)
        fk.fkColumns.add(fkColumn)
        fk.pkColumns.add(pkColumn)
        foreignKeys = foreignKeys.updated(name, fk)
      }
      fkRS.close()
      var numOfFK = 0
      for (fkm <- foreignKeys.values) {
        numOfFK += 1;
        foreignKeyToXML(fkm)
      }
      LOGGER.info("  Foreign Keys: " + numOfFK)
      return foreignKeys.values.toList
    } catch {
      case ex: Exception => {
        if (fkRS != null) {
          fkRS.close()
        }
      }
    }
    null
  }

  private def doIndexes(tableName: String, primaryKeyName: String): List[Index] = {
    val indiceRS = utility.getIndexColumns(dbMetaData.getConnection, schema, tableName)
    var indexMap = Map[String, Index]()
    while (indiceRS.next) {
      val name = indiceRS.getString(INDEX_NAME_RS_COL)
      if (name != null && !((name == primaryKeyName))) {
        var im = indexMap(name)
        if (im == null) {
          val unique = "Y" == indiceRS.getString(INDEX_UNIQUE_RS_COL)
          val direction = indiceRS.getString(INDEX_DIRECTION_RS_COL)
          im = new Index(name, tableName, columns, unique, direction)
        }
        val column = indiceRS.getString(INDEX_COLUMN_RS_COL)
        im.addColumn(column)
        indexMap = indexMap.updated(name, im)
      }
    }
    indiceRS.getStatement.close()
    var numOfIndexes = 0
    for (indexToPrint <- indexMap.values) {
      numOfIndexes += 1
      indexToXML(indexToPrint)
    }
    LOGGER.info("  Indexes: " + numOfIndexes)
    indexMap.values.toList
  }

  private def doPrimaryKey(tableName: String): PrimaryKey = {
    val primaryKeyRS = dbMetaData.getPrimaryKeys(null, schema, tableName)
    var pkName: String = null
    var columns = List[String]()
    while (primaryKeyRS.next) {
      pkName = primaryKeyRS.getString(PK_NAME_RS_COL)
      val pkColumnName = primaryKeyRS.getString(PK_COLUMN_NAME_RS_COL)
      columns = pkColumnName :: columns
    }
    columns = columns.reverse
    if (pkName != null) {
      primaryKeyToXML(pkName, columns)
    }
    primaryKeyRS.getStatement.close()
    new PrimaryKey(tableName, columns)
  }

  private def doProcedures() {
    val conn = dbMetaData.getConnection
    val procedures = utility.getProcedures(conn)
    for (name <- procedures.keySet) {
      xmlWriter.writeStartElement(PROCEDURE_TOKEN)
      xmlWriter.writeAttribute(PROCEDURE_NAME_TOKEN, name)
      xmlWriter.writeStartElement(PROCEDURE_BODY_TOKEN)
      xmlWriter.writeCData(procedures(name))
      xmlWriter.writeEndElement()
      xmlWriter.writeEndElement()
    }
  }

  private def doSequences() {
    val conn = dbMetaData.getConnection
    val rs = utility.getSequences(conn)
    while (rs.next) {
      val sequenceName = rs.getString(1)
      val minValue = rs.getString(2)
      val maxValue = rs.getString(3)
      val incrementBy = rs.getString(4)
      val lastValue = rs.getString(5)
      xmlWriter.writeStartElement(SEQUENCE_TOKEN)
      xmlWriter.writeAttribute(SEQUENCE_NAME_TOKEN, sequenceName)
      xmlWriter.writeAttribute(SEQUENCE_MIN_TOKEN, minValue)
      xmlWriter.writeAttribute(SEQUENCE_MAX_TOKEN, maxValue)
      xmlWriter.writeAttribute(SEQUENCE_INCREMENT_BY_TOKEN, incrementBy)
      xmlWriter.writeCharacters(lastValue)
      xmlWriter.writeEndElement()
    }
    rs.close()
  }

  private def doTriggers(tableName: String) {
    val conn = dbMetaData.getConnection
    val triggersRS = utility.getTriggers(conn, tableName)
    var numOfTriggers = 0
    while (triggersRS.next) {
      numOfTriggers += 1
      val name = triggersRS.getString(1)
      val sqlType = triggersRS.getString(2)
      val event = triggersRS.getString(3)
      val body = triggersRS.getString(4)
      xmlWriter.writeStartElement(TRIGGER_TOKEN)
      xmlWriter.writeAttribute(TRIGGER_NAME_TOKEN, name)
      xmlWriter.writeAttribute(TRIGGER_TYPE_TOKEN, sqlType)
      xmlWriter.writeAttribute(TRIGGER_EVENT_TOKEN, event)
      xmlWriter.writeCData(body)
      xmlWriter.writeEndElement()
    }
    triggersRS.close()
    LOGGER.info("  Triggers: " + numOfTriggers)
  }

  private def indexToXML(im: Index) {
    xmlWriter.writeStartElement(INDEX_TOKEN)
    xmlWriter.writeAttribute(INDEX_NAME_TOKEN, im.name)
    xmlWriter.writeAttribute(INDEX_UNIQUE_TOKEN, im.unique.toString)
    for (col <- im.columns) {
      xmlWriter.writeEmptyElement(INDEX_COLUMN_TOKEN)
      xmlWriter.writeAttribute(INDEX_COLUMN_NAME_TOKEN, col)
    }
    if (im.direction != null && (im.direction == "D")) {
      xmlWriter.writeAttribute(INDEX_DECENDING_TOKEN, "true")
    }
    xmlWriter.writeEndElement()
  }

  private def foreignKeyToXML(fk: ForeignKey) {
    xmlWriter.writeStartElement(FK_TOKEN)
    xmlWriter.writeAttribute(FK_NAME_TOKEN, fk.name)
    xmlWriter.writeAttribute(FK_REFERENCED_TABLE_TOKEN, fk.referencedTable)
    xmlWriter.writeAttribute(FK_ON_DELETE_TOKEN, fk.deleteRule)
    var i = 0
    while (i < fk.fkColumns.size) {
      xmlWriter.writeEmptyElement(FK_REFERENCE_TOKEN)
      xmlWriter.writeAttribute(FK_LOCAL_COLUMN_TOKEN, fk.fkColumns(i))
      xmlWriter.writeAttribute(FK_REFERENCED_COLUMN_TOKEN, fk.pkColumns(i))
      i += 1
    }
    xmlWriter.writeEndElement()
  }

  private def getColumnDefault(tableName: String, columnName: String): String = {
    val conn = dbMetaData.getConnection
    val rs = utility.getColumnDefault(conn, tableName, columnName)
    val defaultValue = if (rs.next()) new String(rs.getBytes(1)) else ""
    rs.close()
    defaultValue
  }

  private def getSchemaVersion: String = {
    val conn = dbMetaData.getConnection
    val rs = utility.getSchemaVersion(conn)
    val version = if (rs.next) new String(rs.getBytes(1)) else ""
    rs.close()
    version
  }

  private def primaryKeyToXML(keyName: String, columnNames: List[String]) {
    xmlWriter.writeStartElement(PRIMARY_KEY_TOKEN)
    xmlWriter.writeAttribute(PRIMARY_KEY_NAME_TOKEN, keyName)
    for (col <- columnNames) {
      xmlWriter.writeEmptyElement(REFERENCE_TOKEN)
      xmlWriter.writeAttribute(LOCAL_COLUMN_TOKEN, col)
    }
    xmlWriter.writeEndElement()
  }

  /**
   * Build an XML representation of the database schema and writes it to the
   * local writer that was passed in from the constructor.
   */
  def schemaToXML() {
    val version = getSchemaVersion
    xmlWriter.writeStartDocument()
    xmlWriter.writeStartElement(SCHEMA_TOKEN)
    xmlWriter.writeAttribute(SCHEMA_VERSION_TOKEN, version)
    if (tableFilter == "%") {
      xmlWriter.writeAttribute(SCHEMA_TABLE_FILTER_TOKEN, "NONE")
    } else {
      xmlWriter.writeAttribute(SCHEMA_TABLE_FILTER_TOKEN, tableFilter)
    }
    val tablesRS = dbMetaData.getTables(null, schema, tableFilter, null)
    while (tablesRS.next) {
      val remarks = tablesRS.getString(REMARKS)
      tableToXML(tablesRS.getString(TABLE_NAME_RS_COL), remarks)
    }
    tablesRS.close()
    doProcedures()
    doSequences()
    xmlWriter.writeEndElement()
    xmlWriter.writeEndDocument()
    xmlWriter.flush()
  }

  /**
   * Build an XML representation of a table and writes it to the local XML
   * writer that was passed in from the constructor.
   * <p/>
   * Tables, Primary and Foreign Keys, Indexes, Check Constraints and Triggers
   * are turned into XML.
   * <p/>
   * When data are transformed to XML tables with suffix _ARC and _REV are
   * not carried over if isSkipDataOnArchiveRevision is set to true.
   *
   * @return the writer used to output
   * @throws Exception
   */
  private def tableToXML(tableName: String, remarks: String): Table = {
    LOGGER.info("Table: " + tableName)
    xmlWriter.writeStartElement(TABLE_TOKEN)
    xmlWriter.writeAttribute(TABLE_NAME_TOKEN, tableName.toLowerCase)
    if (remarks != null && remarks.length > 0) {
      xmlWriter.writeAttribute(TABLE_COMMENT_TOKEN, remarks)
    }

    val primaryKey = doPrimaryKey(tableName)
    val table = new Table(
      tableName,
      doColumns(tableName),
      primaryKey,
      doForeignKeys(tableName),
      doIndexes(tableName, primaryKey.name)
    )
    doCheckConstraints(tableName)
    doTriggers(tableName)
    if (exportData && !tableName.endsWith("_REV") && !tableName.endsWith("_ARC")) {
      var st: Statement = null
      var rs: ResultSet = null
      try {
        st = dbMetaData.getConnection.createStatement
        st.setFetchSize(RECORDS_TO_FETCH)
        st.execute(utility.generateSelectQuery(schema, table))
        rs = st.getResultSet
        doRow(rs, table)
        st.close()
      } finally {
        if (rs != null) {
          rs.close()
        }
        if (st != null) {
          st.close()
        }
      }
    }
    xmlWriter.writeEndElement()
    xmlWriter.flush()
    table
  }

  private def doRow(rs: ResultSet, table: Table) {
    var sb: StringBuilder = null
    var rowCounter = 0
    while (rs.next) {
      sb = new StringBuilder
      rowCounter += 1
      val columns = table.columns
      var i = 0
      while (i < columns.size) {
        val col = columns(i)
        val columnName = col.name
        var value: String = null
        var sqlType = ""
        if ("CLOB" == col.sqlType) {
          val clobBuilder = new StringBuilder
          sqlType = "C"
          val clob = rs.getClob(columnName)
          if (clob != null) {
            val r = new BufferedReader(clob.getCharacterStream)
            Stream.continually { r.read() }
                .takeWhile { _ != -1 }
                .foreach { ch =>
              if ('"' == ch.asInstanceOf[Char]) {
                clobBuilder.append("\"\"")
              } else {
                clobBuilder.append(ch.asInstanceOf[Char])
              }
            }
            value = clobBuilder.toString()
            r.close()
          }
        } else if ("BLOB" == col.sqlType) {
          sqlType = "B"
          val blob = rs.getBlob(columnName)
          value = table.name + "-" + rowCounter
          if (blob != null && blob.length > 0) {
            val os = new BufferedOutputStream(new FileOutputStream(new File(value)))
            val is = new BufferedInputStream(blob.getBinaryStream)
            Stream.continually { is.read() }
                .takeWhile { _ != -1 }
                .foreach { os.write(_) }
            os.flush()
          }
        } else if ("TIMESTAMP" == col.sqlType) {
          sqlType = "T"
          val ts = rs.getTimestamp(columnName)
          if (ts != null) {
            value = df.format(ts.getTime)
          }
        } else {
          sqlType = "V"
          value = rs.getString(columnName)
          if (value != null) {
            value = value.replaceAll("\"", "\"\"")
          }
        }
        if (value == null) {
          sb.append("n")
        } else {
          sb.append(sqlType).append("\"").append(value).append("\"")
        }
        if (i < columns.size - 1) {
          sb.append(",")
        }
        i += 1
      }
      xmlWriter.writeStartElement(ROW_TOKEN)
      xmlWriter.writeCData(sb.toString())
      xmlWriter.writeEndElement()
      if ((rowCounter % RECORDS_TO_FETCH) == 0) {
        xmlWriter.flush()
        LOGGER.info("Exported " + rowCounter + " rows.")
      }
    }
  }
}
