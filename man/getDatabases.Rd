\name{getDatabases}
\alias{getDatabases}
\alias{getDatabases.MySQLManager}
\alias{getDatabases.MySQLConnection}
\title{ Get a list of Existing Databases}
\description{
    This returns a list of existing databases accessible with a database
    manager or a database connection.
}
\usage{
getDatabases(object, ...)
}
\arguments{
 \item{object}{ an object of class MySQLManager or MySQLConnection }
}
\value{
    A list with a single component giving the database names.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{\code{\link{MySQLManager}}, \code{\link{dbConnect}}}
\examples{
m <- MySQL()
getDatabases(m)
con <- dbConnect(m, user = "nobody", dbname = "test")
getDatabases(con)
close(con)
}
\keyword{ interface }
