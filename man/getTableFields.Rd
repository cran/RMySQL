\name{getTableFields}
\alias{getTableFields}
\title{ Show Fields From a Table}
\description{
    This shows information about the columns from a table. If dbname is
    missing, the default database for the connection is used. If object
    is of class MySQLResultSet, table and dbname are ignored and the
    table associated with the resultSet is used.
}
\usage{
getTableFields(object, table, dbname)
}
\arguments{
 \item{object}{ object of class MySQLConnection or MySQLResultSet}
 \item{table}{ name of the table}
 \item{dbname}{ name of the database for the table }
}
\value{
    List containing information about the table columns.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody")
tableNames <- getTables(con, dbname = "test")[[1]]
if(length(tableNames) > 1)
    getTableFields(con, table = tableNames[1], dbname = "test")
close(con)
}
\keyword{interface}
