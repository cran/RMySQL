\name{methods.MySQL}
\alias{SQLDataType.MySQLConnection}
\alias{SQLDataType.MySQLManager}
\alias{SQLDataType.default}
\alias{as.MySQLConnection}
\alias{as.MySQLManager}
\alias{as.MySQLResultSet}
\alias{as.character.dbObjectId}
\alias{as.integer.MySQLConnection}
\alias{as.integer.MySQLManager}
\alias{as.integer.MySQLResultSet}
\alias{as.integer.dbObjectId}
\alias{as.numeric.dbObjectId}
\alias{assignTable.MySQLConnection}
\alias{close.MySQLConnection}
\alias{close.MySQLResultSet}
\alias{close.dbConnection}
\alias{close.resultSet}
\alias{commit.MySQLConnection}
\alias{dbConnect.MySQLConnection}
\alias{dbConnect.MySQLManager}
\alias{dbConnect.default}
\alias{dbExec.MySQLConnection}
\alias{dbExecStatement.MySQLConnection}
\alias{describe.MySQLConnection}
\alias{describe.MySQLManager}
\alias{describe.MySQLResultSet}
\alias{execStatement.MySQLConnection}
\alias{existsTable.MySQLConnection}
\alias{existsTable.dbConnection}
\alias{fetch.MySQLResultSet}
\alias{format.MySQLConnection}
\alias{format.MySQLManager}
\alias{format.MySQLResultSet}
\alias{format.dbObjectId}
\alias{getConnection.MySQLConnection}
\alias{getConnection.MySQLResultSet}
\alias{getConnection.default}
\alias{getConnections.MySQLManager}
\alias{getCurrentDatabase.MySQLConnection}
\alias{getDatabases.MySQLConnection}
\alias{getException.MySQLConnection}
\alias{getException.MySQLResultSet}
\alias{getFields.MySQLResultSet}
\alias{getInfo.MySQLConnection}
\alias{getInfo.MySQLManager}
\alias{getInfo.MySQLResultSet}
\alias{getManager.MySQLConnection}
\alias{getManager.MySQLResultSet}
\alias{getNumCols.default}
\alias{getResultSets.MySQLConnection}
\alias{getRowCount.MySQLResultSet}
\alias{getRowsAffected}
\alias{getRowsAffected.MySQLResultSet}
\alias{getStatement.MySQLResultSet}
\alias{getTable.MySQLConnection}
\alias{getTable.dbConnection}
\alias{getTableFields.MySQLConnection}
\alias{getTableFields.MySQLResultSet}
\alias{getTableIndices.MySQLConnection}
\alias{getTables.MySQLConnection}
\alias{getVersion.MySQLConnection}
\alias{getVersion.MySQLManager}
\alias{hasCompleted.MySQLResultSet}
\alias{load.MySQLManager}
\alias{load.default}
\alias{loadManager.MySQLManager}
\alias{NEW.MySQLObject}
\alias{NEW.dbObjectId}
\alias{NEW.MySQLManager}
\alias{NEW.MySQLConnection}
\alias{NEW.MySQLResultSet}
\alias{NEW.default}
\alias{newConnection.MySQLManager}
\alias{print.MySQLConnection}
\alias{print.MySQLManager}
\alias{print.MySQLResultSet}
\alias{print.dbObjectId}
\alias{quickSQL.MySQLConnection}
\alias{removeTable.MySQLConnection}
\alias{removeTable.dbConnection}
\alias{rollback.MySQLConnection}
\alias{show.default}
\alias{unload.MySQLManager}

\title{Support MySQL methods}
\description{
  MySQL support methods. For details on what it does, see the 
  documentation of the generic function.
}
\usage{
assignTable.MySQLConnection(con, name, value, field.types, row.names, overwrite, append, ...)
}
\arguments{
  \item{con}{an MySQL connection.}
  \item{name}{the name of the MySQL table to create, overwrite, or append to.}
  \item{value}{a data.frame to be exported to the DBMS}
  \item{row.names}{field name in \code{value} to be mapped to a
        \code{row_names} MySQL column. By default \code{row.names(value)}
        is added as the first column of the \code{value} data.frame,
        and this expanded data.frame is transferred to the database. 
        If the argument \code{row.names} is \code{NULL}, \code{FALSE} or 0 
        then no row names are attached to \code{value}.}
  \item{field.types}{a list with as many elements as fields in the
        target MySQL \code{table}.  Each member of the list maps the
        corresponding field in \code{value} into a MySQL data type.
        By default, this argument is built by invoking
        \code{\link{SQLDataType}} on each element of \code{value}, possibly
        after adding a \code{row.names} field (not attribute) to \code{value}
        (see previous argument).}
  \item{overwrite}{logical specifying whether to overwrite an existing 
        MySQL table or not.}      
  \item{append}{logical specifying whether to append to an existing 
        MySQL table or not.}      
  \item{\dots}{additional arguments are silently ignored.}
}
\details{
  See \code{help(generic)} for a description of the
  functionality that this method provides in the
  context of the MySQL driver.
}
\seealso{
On database managers:

\code{\link{dbManager}}
\code{\link{MySQL}}
\code{\link{load}}
\code{\link{unload}}

On connections, SQL statements and resultSets:

\code{\link{dbExecStatement}}
\code{\link{dbExec}}
\code{\link{fetch}}
\code{\link{quickSQL}}

On transaction management:

\code{\link{commit}}
\code{\link{rollback}}

On meta-data:

\code{\link{describe}}
\code{\link{getVersion}}
\code{\link{getDatabases}}
\code{\link{getTables}}
\code{\link{getFields}}
\code{\link{getCurrentDatabase}}
\code{\link{getTableIndices}}
\code{\link{getException}}
\code{\link{getStatement}}
\code{\link{hasCompleted}}
\code{\link{getRowCount}}
\code{\link{getAffectedRows}}
\code{\link{getNullOk}}
\code{\link{getInfo}}
}

\examples{\dontrun{
m <- dbManager("MySQL")
con <- dbConnect(m, user="opto", pass="pure-light", dbname="opto")
}
}
\keyword{interface}
\keyword{database}
% vim: syntax=tex
