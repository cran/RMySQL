\name{getTableIndeces}
\alias{getTableIndeces}
\title{ Show Indeces From a Table}
\description{
    This shows the indeces from a table. If dbname is missing, the
    default database for the connection is used.
}
\usage{
getTableIndeces(object, table, dbname)
}
\arguments{
 \item{object}{ object of class MySQLConnection }
 \item{table}{ name of the table}
 \item{dbname}{ name of the database for the table }
}
\value{
    List containing information about the table indeces.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody")
tableNames <- getTables(con, dbname = "test")[[1]]
if(length(tableNames) > 1)
    getTableIndeces(con, table = tableNames[1], dbname = "test")
close(con)
}
\keyword{interface}
