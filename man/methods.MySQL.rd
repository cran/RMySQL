\name{methods.MySQL}
\alias{SQLDataType.MySQLConnection}
\alias{SQLDataType.MySQLManager}
\alias{SQLDataType.default}
\alias{as.MySQLConnection}
\alias{as.MySQLManager}
\alias{as.MySQLResultSet}
\alias{as.integer.MySQLConnection}
\alias{as.integer.MySQLManager}
\alias{as.integer.MySQLResultSet}
\alias{as.integer.dbObjectId}
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
\alias{new.MySQLManager}
\alias{new.MySQLObject}
\alias{new.MySQLResultSet}
\alias{new.dbObjectId}
\alias{new.default}
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
<generic>.MySQLManager(obj, ...)   
}
\arguments{
  \item{<generic>}{refers to an actual generic function name, e.g., 
        \code{dbConnect}}
  \item{obj}{is some kind of MySQL object}
  \item{\dots}{additional arguments}
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
