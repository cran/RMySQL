\name{dbExecStatement}
\alias{dbExecStatement}
\title{ Execute an SQL Statement}
\description{
    Execute an SQL statement and return a resultSet object. The
    resultSet object can be used to fetch rows from the result of the
    statement.
}
\usage{
dbExecStatement(con, statement)
}
\arguments{
 \item{con}{ a MySQLConnection }
 \item{statement}{ a character of length one - the statement to execute }
}
\value{
    An object of class MySQLResultSet
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{describe}},
    \code{\link{fetch}}, \code{\link{dbExec}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
describe(rs, verbose = TRUE)
fetch(rs, n = -1)
close(rs)
close(con)
}
\keyword{interface}
