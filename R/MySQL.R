## $Id$

## Class: dbObjectid
##
## This helper class allows us to conviniently implement all dbObjects
## (Manger, connections, resultSets, etc.) as identifiers into 
## actual remote database objects.  The class needs to be VIRTUAL 
## to avoid coercion (green book, p.293).

new.dbObjectId <- function(Id = 0)
{
  Id <- as.integer(Id)
  class(Id) <- "dbObjectId"
  Id
}

as.integer.dbObjectId <- function(object)
{
  NextMethod("as.integer")
}

print.dbObjectId <- 
show.dbObjectId <- 
function(object)
{
  str <- paste(class(object), " id = ", format(object), sep="")
  if(isIdCurrent(object))
    cat(str, "\n")
  else
    cat("Expired", str, "\n")
  invisible(NULL)
}

## Class: dbManager

new.MySQLManager <- function(Id)
{
  out <- list(Id = new.dbObjectId(Id))
  class(out) <- c("MySQLManager", "dbManager")
  out
}

as.integer.MySQLManager <- function(object)
{
  object <- object$Id
  NextMethod("as.integer")
}

MySQLManager <- function(max.con=16, fetch.default.rec = 5000, force.reload=F)
## create a MySQL database connection manager.  By default we allow
## up to "max.con" connections and single fetches of up to "fetch.default.rec"
## records.  These settings may be changed by re-loading the driver
## using the "force.reload" = T flag (note that this will close all 
## currently open connections).
## Returns an object of class "MySQLManger".  
## Note: This class is a singleton.
{
  id <- load.MySQLManager(max.con, fetch.default.rec, force.reload)
  new.MySQLManager(id)
}

MySQL <- MySQLManager

loadManager.MySQLManager <- function(dbMgr, ...)
  load.MySQLManager(...)

## Class: dbConnection
new.MySQLConnection <- function(Id)
{
  out <- list(Id = new.dbObjectId(Id))
  class(out) <- c("MySQLConnection", "dbConnection")
  out
}

as.integer.MySQLConnection <- function(object)
{
  object <- object$Id
  NextMethod("as.integer")
}

dbConnect.MySQLManager <- function(dbMgr, ...)
{
  id <- newConnection.MySQLManager(dbMgr, ...)
  new.MySQLConnection(Id = id)
}

quickSQL <- function(con, statement, ...)
	   UseMethod("quickSQL")

getException.MySQLConnection <- function(object)
{
  id <- as.integer(object)
  .Call("S_MySQL_getException", id)
}

dbExecStatement.MySQLConnection <- function(con, statement, ...)
{
  if(!is.character(statement))
    stop("non character statement")
  execStatement.MySQLConnection(con, statement, ...)
}

dbExec.MySQLConnection <- function(con, statement, ...)
{
  if(!is.character(statement))
    stop("non character statement")
  quickSQL(con, statement, ...)
}

## Class: resultSet

new.MySQLResultSet <- function(Id)
{
  out <- list(Id = new.dbObjectId(Id))
  class(out) <- c("MySQLResultSet", "resultSet")
  out
}

as.integer.MySQLResultSet <- function(object)
{
  object <- object$Id
  NextMethod("as.integer")
}

getDBconnection.MySQLResultSet <- function(object)
{
  new("MySQLConnection", Id=object$Id[1:2])
}

getStatement.MySQLResultSet <- function(object)
{
  getInfo.MySQLResultSet(object, "statement")
}

getFields.MySQLResultSet <- function(object)
{
  flds <- getInfo.MySQLResultSet(object, "fieldDescription")
  flds$Sclass <- .Call("S_DBI_SclassNames", flds$Sclass)
  flds$type <- .Call("S_MySQL_fieldTypeNames", flds$type)
  data.frame(flds)
}

getRowsAffected.MySQLResultSet <- function(object)
  getInfo.MySQLResultSet(object, "rowsAffected")

getRowCount.MySQLResultSet <- function(object)
  getInfo.MySQLResultSet(object, "rowCount")

hasCompleted.MySQLResultSet <- function(object)
  getInfo.MySQLResultSet(object, "completed")

getException.MySQLResultSet <- function(object)
{
  id <- as.integer(object)
  .Call("S_MySQL_getException", id[1:2])
}

getCurrentDatabase.MySQLConnection <- function(object, ...)
{
  quickSQL(object, "select DATABASE()")
}

## the following depends on MySQL having a "nobody" user that requires 
## no password (this is the default config)
getDatabases.MySQLManager <- function(object, ...)
{
  on.exit(close(con))
  con <- dbConnect(object, user = "nobody", ...)
  quickSQL(con, "show databases")
}

getDatabases.MySQLConnection <- function(object, ...)
{
  quickSQL(object, "show databases")
}

getTables.MySQLConnection <- function(object, dbname, ...)
{
  if (missing(dbname))
    quickSQL(object, "show tables")
  else quickSQL(object, paste("show tables from", dbname))
}

getTableFields.MySQLResultSet <- function(object, table, dbname, ...)
{
  getFields(object)
}

getTableFields.MySQLConnection <- function(object, table, dbname, ...)
{
  if (missing(dbname))
    cmd <- paste("show columns from", table)
  else cmd <- paste("show columns from", table, "from", dbname)
  quickSQL(object, cmd)
}

getTableIndeces.MySQLConnection <- function(object, table, dbname, ...)
{
  if (missing(dbname))
    cmd <- paste("show index from", table)
  else cmd <- paste("show index from", table, "from", dbname)
  quickSQL(object, cmd)
}





