## $Id: MySQLsupport.S,v 1.1.1.1 2000/01/04 21:38:26 root Exp $
##
## S-MySQL Support functions
##

show <- print

"load.MySQLManager" <- 
function(max.con = 16, fetch.default.rec = 5000, force.reload=F)
## return a manager id
{
  config.params <- as.integer(c(max.con, fetch.default.rec))
  force <- as.logical(force.reload)
  .Call("S_MySQL_init", config.params, force)
}

"describe.MySQLManager" <-
function(object, verbose = F, ...)
## Print out nicely a brief description of the connection Manager
{
  out <- getInfo.MySQLManager()
  show(object)
  cat("  Default records per fetch:", out$"fetch_default_rec", "\n")
  cat("  Conn. processed:", out$counter, "\n")
  cat("  Max  connections:", out$length, "\n")
  cat("  Open connections:", out$"num_con", "\n")
  if(verbose && !is.null(out$connectionIds)){
    for(i in seq(along = out$connectionIds)){
      cat("   ", i, " ")
      show(out$connectionIds[[i]])
    }
  }
  invisible(NULL)
}

"unloadManager.MySQLManager"<-
function(dbMgr, ...)
{
  mgrId <- as.integer(dbMgr)
  .Call("S_MySQL_close", mgrId)
}

"getInfo.MySQLManager" <- 
function(object)
{
  out <- .Call("S_MySQL_managerInfo")
  mgrId <- out$managerId
  conIds <- vector("list", length = out$"num_con")
  for(i in seq(along = out$connectionIds)){
    obj <- new.MySQLConnection(Id = c(mgrId, out$connectionIds[i]))
    conIds[[i]] <- obj
  }
  out$connectionIds <- conIds
  mgrId <- new.MySQLManager(Id = out$managerId)
  out$managerId <- mgrId
  out
}

"getVersion.MySQLManager" <-
function(dbMgr)
## TODO: what should we return? The S-MySQL version? The S-DBI?
## the version of the underlying MySQL?  Probably all of the above.
{
  return("0.2")
}

"newConnection.MySQLManager" <- 
function(dbMgr, dbname ="", username="", password="", host="",  
         unix.socket = "", port = 0, client.flag = 0)
{
  con.params <- c(username, password, host, dbname, unix.socket, 
	      as.character(port), as.character(client.flag))
  con.params <- as.character(con.params)
  mgrId <- as.integer(dbMgr)
  .Call("S_MySQL_newConnection", mgrId, con.params)
}

## functions/methods not implementable
"commit.MySQLConnection" <- 
"rollback.MySQLConnection" <- 
function(con) 
{
  warning("MySQL does not support transactions")
}

"describe.MySQLConnection" <- 
function(object, verbose = F, ...)
{
  Id <- as.integer(object)
  out <- .Call("S_MySQL_connectionInfo", Id)
  info <- out[[1]]
  rsId <- out[[2]]
  show(object)
  cat("  User:", info[1], "\n")
  cat("  Host:", info[2], "\n")
  cat("  Dbname:", info[3], "\n")
  cat("  Connection type:", info[4], "\n")
  if(length(rsId)==1 && rsId >= 0)
    rsId <- new.MySQLResultSet(Id = as.integer(c(Id, rsId)))
  else
    rsId <- NULL
  if(is.null(rsId))
    cat("  No resultSet available\n")
  else {
    if(verbose)
      describe(rsId, verbose = verbose)
    else
      show(rsId)
  }
  invisible(NULL)
}

"close.MySQLConnection" <- 
function(con, ...)
{
  conId <- as.integer(con)
  .Call("S_MySQL_closeConnection", conId)
}

"getInfo.MySQLConnection" <-
function(object)
{
  id <- as.integer(object)
  out <- .Call("S_MySQL_connectionInfo", id)
  list(connectionId = object, 
       user = out[1], host = out[2], dbname = out[3], con.type = out[4])
}
       
"execStatement.MySQLConnection" <- 
function(con, statement)
## submits the sql statement to MySQL and creates a resultSet.
## Resturns a MySQLresultSet.
{
  ## Currently we can only have one statemnt open
  conId <- as.integer(con)
  statement <- as.character(statement)
  rsId <- .Call("S_MySQL_exec", conId, statement)
  out <- new.MySQLResultSet(Id = rsId)
  out
}

## helper function: it exec's *and* retrieves a statement. It should
## be named somehting else.
quickSQL.MySQLConnection <- function(con, statement)
{
  rs <- dbExecStatement(con, statement)
  res <- fetch(rs, n = -1)
  if(hasCompleted(rs))
    close(rs)
  else 
    warning("pending rows")
  res
}

"fetch.MySQLResultSet" <- 
function(resultSet, n=500)   
## Fetch at most n records from the opened resultSet (n = -1 means
## all records).  The returned object is a data.frame.
## Note: The method hasCompleted() on the resultSet tells you whether
## or not there are pending records to be fetched. See also the methods
## describe(), getFieldDescrition(), getRowCount(), getAffectedRows(),
## getDBConnection(), getException().
## 
## TODO: Make sure we don't exhaust all the memory, or generate
## an object whose size exceeds option("object.size").  Also,
## are we sure we want to return a data.frame?
{    
  n <- as.integer(n)
  rsId <- as.integer(resultSet)
  rel <- .Call("S_MySQL_fetch", rsId, nrec = n)
#data.frame(rel, check.names = F)
  rel
}

"getInfo.MySQLResultSet" <- 
function(object, what)
{
   rsId <- as.integer(object)
   if(missing(what))
     what <- c("statement", "fieldDescription", "rowsAffected",
	       "rowCount", "completed")
   what <- as.character(what)
   n <- length(what)
   out <- vector("list", length = n)
   for(i in seq(along = what)){
     w <- what[i]
     out[[i]] <- .Call("S_MySQL_resultSetInfo", rsId, w)
   }
   if(n == 1)
     out <- out[[1]]
   else
     names(out) <- what
   out
}

"describe.MySQLResultSet" <- 
function(object, verbose = F, ...)
{
  if(!isIdCurrent(object)){
    show(object)
    invisible(return(NULL))
  }
  show(object)
  cat("  Statement:", getStatement(object), "\n")
  cat("  Has completed?", if(hasCompleted(object)) "yes" else "no", "\n")
  cat("  Rows fetched:", getRowCount(object), "\n")
  cat("  Affected rows:", getRowsAffected(object), "\n")
  flds <- getFields(object)
  if(verbose && !is.null(flds)){
    cat("  Fields:\n")  
    out <- print(getFields(object))
  }
  invisible(NULL)
}

"close.MySQLResultSet" <- 
function(con)
{
  rsId <- as(con, "integer")
  .Call("S_MySQL_closeResultSet", rsId)
}

"isIdCurrent" <- 
function(object)
## verify that dbObjectId refers to a currently open/loaded database
{ 
  object <- as(object, "integer")
  .Call("S_MySQL_validHandle", object)
}


## work-around.  For some reason I don't understand, if I define
## a class extending both {"dbManager", "dbConnection", "dbResultSet"}
## and "dbObjectId" (this one also as a VIRTUAL class) S doesn't select 
## the proper show methods for "dbObjectId". Thus the following 
## function to be re-used by show() with
## " MySQL{Manger,Connection,ResultSet}" Grrr!
"format.dbObjectId" <- 
function(x)
{
  id <- as.integer(x)
  paste("(", paste(id, collapse=","), ")", sep="")
}
