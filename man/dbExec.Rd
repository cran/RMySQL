\name{dbExec}
\alias{dbExec}
\title{ Execute and Get Value for an SQL Statement}
\description{
    Execute an SQL statement and return the value.
}
\usage{
dbExec(con, statement)
}
\arguments{
 \item{con}{ a MySQLConnection }
 \item{statement}{ a character of length one - the statement to execute }
}
\value{
    A list giving results of the query.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{describe}},
    \code{\link{fetch}}, \code{\link{dbExecStatement}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
dbExec(con, "SHOW TABLES")
close(con)
}
\keyword{interface}
