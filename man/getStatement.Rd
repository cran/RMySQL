\name{getStatement}
\alias{getStatement}
\title{ Show the Statement Associated With a resultSet}
\description{
    This shows the SQL statement that was used to create the resultSet.
}
\usage{
getStatement(object)
}
\arguments{
 \item{object}{ object of class MySQLResultSet }
}
\value{
    Character of length one
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
fetch(rs, n = -1)
getStatement(rs)
close(con)
}
\keyword{interface}
