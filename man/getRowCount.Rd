\name{getRowCount}
\alias{getRowCount}
\title{ Get the Number of Rows in a resultSet}
\description{
    This gets the number of rows in a resultSet. Gives 0 if no row has
    been fetched.
}
\usage{
getRowCount(object)
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
getRowCount(rs)
fetch(rs, n = -1)
getRowCount(rs)
close(con)
}
\keyword{interface}
