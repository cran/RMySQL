\name{dbManager}
\alias{dbManager}
\title{
  Instantiate a database manager
}
\description{
This function creates an object that allows you to
connect to the Relational Database Systems (RDBMS) 
specified in its argument.
}
\usage{
dbManager(mgr, ...)
}
\arguments{
\item{mgr}{
a character string specifying the RDBMS, e.g., "MySQL", 
"Oracle", "Informix". 
}
\item{\dots }{
additional parameters may be specified for the actual database engine.
E.g., the MySQL implementation allows you to specify the maximum 
number of open connection and a default maximum number of records 
to be transferred from the database.
See the individual manager functions for details,
e.g., \code{MySQL}, 
\code{Oracle}.
}
}
\value{
An object that extends \code{dbManager} and 
\code{dbObjectId} in a database-specific manner.
For instance \code{dbManager("MySQL")} produces
an object of class \code{MySQLManager} and 
is equivalent to using \code{MySQL}.
Similarly \code{dbManager("Oracle")} produces
an \code{OracleManager} object and its equivalent
to invoking \code{Oracle}.

This object is required to create connections
to one or several database engines.
}
\section{Side Effects}{
The R/S client part of the database communication is initialized,
connecting to the database engine needs to be done through
calls to \code{dbConnect}.
}
\details{
This object is a singleton, that is, if you invoke this function
again, it will return the same initialized object. 
}
\section{References}{
See the Omega Project for Statistical Computing
at \url{http://www.omegahat.org}
for more details on the R/S database interface.
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
# create a MySQL instance and create one connection.
> m <- dbManager("MySQL")
> m
MySQLManager id = (7269) 

# open the connection using user, passsword, etc., as
# specified in the file \file{\$HOME/.my.cnf}
> con <- dbConnect(m)    

# Let's look at the status of the manager
> describe(m, verbose = F)   
MySQLManager id = (7269) 
  Max  connections: 16 
  Conn. processed: 1 
  Default records per fetch: 500 
  Open connections: 1 

# Run an SQL statement by creating first a resultSet object
> rs <- dbExecStatement(con, 
         statement = "SELECT w.laser_id, w.wavelength, p.cut_off 
                      FROM WL w, PURGE P
                      WHERE w.laser_id = p.laser_id
                      SORT BY w.laser_id")
> rs
MySQLResultSet id = (12629,1,3)

# we now fetch records from the restulSet into a data.frame
> data <- fetch(rs, n = -1)   # extract all rows
> dim(data)
[1] 1779  18

# Extract meta-data information.  What MySQL databases are there 
# available on host "wyner"
> getDatabases(m, host = "wyner")
   Database 
1     mysql
2      opto
3      test
4 iptraffic
5     fraud

> # What tables are there in the "opto" database? 
> dbTables(m, dbname = "opto", host = "wyner")
  Tables in opto 
1           PBCT
2          PURGE
3             WL
4          liv25
5          liv85
}
}
\keyword{RS-DBI}
\keyword{MySQL}
\keyword{databases}
\keyword{RDBMS}
\keyword{manip}
% docclass is function
% Converted by Sd2Rd version 1.15.2.1.
