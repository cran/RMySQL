## S-Database Interface
##
## $Id: S-DBI.S,v 1.1.1.1 2000/01/04 21:38:26 root Exp $

## Define all the classes and methods to be used by an implementation
## of the S-DataBase Interface.  Mostly, these classes are virtual
## and each driver should extend them to provide the actual implementation.
## See the files Sora.S and S_MySQL.S for the Oracle and MySQL
## implementations, respectively.

## Class: dbManager 
## This class identifies the DataBase Management System (oracle, informix, etc)

dbManager <- function(dbMgr, ...)
{
  new(dbMgr, ...)
}

loadManager <- function(dbMgr, ...)
  UseMethod("loadManager")

unloadManager <- function(dbMgr, ...)
  UseMethod("unloadManager")

getVersion <- function(dbMgr, ...)
  UseMethod("getVersion")

## the following is a generic for describing databases, connections, 
## resultSets, etc, and getting meta-data info for the respective object.

getInfo <- function(object)
  UseMethod("getInfo")

describe <- function(object, verbose = F, ...)
  UseMethod("describe")

## Class: dbConnections

dbConnection <- function(dbMgr, ...)
  UseMethod("dbConnection")

dbConnect <- function(dbMgr, ...)
  UseMethod("dbConnect")

dbExecStatement <- function(con, statement, ...)
  UseMethod("dbExecStatement")

dbExec <- function(con, statement, ...)
  UseMethod("dbExec")

getResultSet <- function(con, ...)
  UseMethod("getResultSet")

commit <- function(con, ...)
  UseMethod("commit")

rollback <- function(con, ...)
  UseMethod("rollback")

callProc <- function(con, ...)
  UseMethod("callProc")

close <- function(con, ...)
  UseMethod("close")

close.dbConnection <- function(con, type) NULL

## Class: resultSet
## Note that we define a resultSet as the result of *any* SQL statement,
## not just SELECT's.

fetch <- function(resultSet, n, ...)
  UseMethod("fetch")

setDataMappings <- function(resultSet, ...)
  UseMethod("setDataMappings")

close.resultSet <- function(con, type) NULL

getDBconnection <- function(object)
  UseMethod("getDBconnection")

getFields <- function(object)
  UseMethod("getFields")

getStatement <- function(object)
  UseMethod("getStatement")

getRowsAffected <- function(object)
  UseMethod("getRowsAffected")

getRowCount <- function(object)
  UseMethod("getRowCount")

getNullOk <- function(object)
  UseMethod("getNullOk")

hasCompleted <- function(object)
  UseMethod("hasCompleted")

getException <- function(object)
  UseMethod("getException")

## Meta-data:
##
## It may be possible to get the following meta-data from the
## dbManager object iteslf, or it may be necessary to get it from 
## a dbConnection; this is likely to be driver specific.
##
## TODO: what to do about permissions? privileges? users? Some 
## databses, e.g., mSQL, do not support multiple users.  Can we
## get away without these?
##
## The basis for defining the following meta-data is to provide the 
## basics for writing methods for attach(db) and related methods 
## (objects, exist, assign, remove) so that we can even abstract
## from SQL and the S-Database interface itself.

getCurrentDatabase <- function(object, ...)
  UseMethod("getCurrentDatabase")

getDatabases <- function(object, ...)
  UseMethod("getDatabases")

getTables <- function(object, dbname, ...) 
  UseMethod("getTables")

getTableFields <- function(object, table, dbname, ...)
  UseMethod("getTableFields")

getTableIndeces <- function(object, table, dbname, ...) 
  UseMethod("getTableIndeces")









