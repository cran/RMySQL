\name{dbConnect}
\alias{dbConnect}
\title{ Connect to a database}
\description{
    dbConnect creates and returns a connection to a database. See method
    documentation for details.
}
\usage{
dbConnect(dbMgr, ...)
}
\arguments{
 \item{dbMgr}{ an object inheriting from class dbManager.}
 \item{\dots}{ see method description}
}
\value{
    dbConnect returns an object of class dbConnection.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnection.MySQLMananger}}}
\keyword{interface}
