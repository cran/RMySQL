\name{describe}
\alias{describe}
\alias{describe.MySQLManager}
\alias{describe.MySQLConnection}
\alias{describe.MySQLResultSet}
\title{Describe a Database Driver Object}
\description{
    This is a generic function for describing various database driver objects.
}
\usage{
describe(object, verbose=F)
}
\arguments{
 \item{object}{ an object of class MySQLManager, MySQLConnection or
     MySQLResultSet }
 \item{verbose}{ logical flag for verbose mode }
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{MySQLManager}}, \code{\link{dbConnect}},
    \code{\link{dbExecStatement}}, \code{\link{close}}}
\examples{
m <- MySQL()
describe(m)
con <- dbConnect(m, user = "nobody", dbname = "test")
describe(con)
rs <- dbExecStatement(con, "SHOW TABLES")
describe(rs)
close(rs)
close(con)
}
\keyword{interface}
