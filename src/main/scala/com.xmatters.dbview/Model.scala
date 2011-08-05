package com.xmatters.dbview

abstract class DatabaseObject(val name: String)

class CheckConstraint(val name: String, val checkClause: String) extends DatabaseObject(name)

class Column(
    val name: String,
    val sqlType: String, val size: Int, val nullable: Boolean, val defaultValue: String,
    val comments: String)
    extends DatabaseObject(name)


class ForeignKey(
    val name: String,
    val referencedTable: String, val fkColumns: List[String], val pkColumns: List[String],
    val deleteRule: String)
    extends DatabaseObject(name)

class Index(
    val name: String,
    val tableName: String, val columns: List[String],
    val unique: Boolean, val direction: String)
    extends DatabaseObject(name)

class PrimaryKey(val name: String, val columns: List[String]) extends DatabaseObject(name)

class Table(
    val name: String,
    val columns: List[Column],
    val primaryKey: PrimaryKey, val foreignKeys: List[ForeignKey],
    val indices: List[Index])
    extends DatabaseObject(name)
