\name{getRowsAffected}
\alias{getRowsAffected}
\title{ Get the Number of Rows Affected by a resultSet}
\description{
    This gets the number of rows affected by a resultSet. Returns -1
    when the statement resulted in an error.
}
\usage{
getRowsAffected(object)
}
\arguments{
 \item{object}{ object of class MySQLResultSet }
}
\value{
    Integer of length one
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
fetch(rs, n = -1)
getRowsAffected(rs)
close(rs)
close(con)
}
\keyword{interface}
