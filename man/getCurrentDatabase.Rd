\name{getCurrentDatabase}
\alias{getCurrentDatabase}
\title{ Get Name of the Current Database}
\description{
    For a MySQLConnection, this returns its current database.
}
\usage{
getCurrentDatabase(object)
}
\arguments{
 \item{object}{ object of class MySQLConnection}
}
\value{
    A list with a component giving name of the database
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
getCurrentDatanase(con)
close(con)
}
\keyword{interface}
