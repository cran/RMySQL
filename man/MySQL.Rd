\name{MySQL}
\alias{MySQL}
\alias{MySQLManager}
\title{Create a MySQL Database Connection Manager}
\description{
    \code{MySQLManager} returns a database connection manager for MySQL.
    
    \code{MySQL} is an alias for \code{MySQLManager}.
}
\usage{
MySQLManager(max.con=16, fetch.default.rec=5000, force.reload=F)
}
\arguments{
 \item{max.con}{ number of connections to allow}
 \item{fetch.default.rec}{ number of records to fetch by default }
 \item{force.reload}{ logical flag to force reloading the driver. Note
     that this will close all currently open connections}
}
\value{
    An object of class MySQLManager inheriting from the class
    dbManager. It is a list with a single component \code{Id} of class
    dbObjectId.
}
\references{\url{http://www.mysql.com/}}
\author{David James and Saikat DebRoy}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
getDatabases(m)
con <- dbConnect(m, user = "nobody", dbname = "test")
getTables(con)
close(con)
}
\keyword{interface}
