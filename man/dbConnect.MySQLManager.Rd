\name{dbConnect.MySQLManager}
\alias{dbConnect.MySQLManager}
\alias{newConnection.MySQLManager}
\title{ Connect to a MySQL Database}
\description{
    dbConnect.MySQLManager creates and returns a connection to a MySQL
    database.

    newConnection.MySQLManager is a low level function not accessed by
    users directly.
}
\usage{
dbConnect.MySQLManager(dbMgr, ...)
newConnection.MySQLManager(dbMgr, dbname = "", username, password = "",
                           host = "", unix.socket = "", port = 0,
                           client.flag = 0)
}
\arguments{
 \item{dbMgr}{ an object inheriting from class MySQLManager.}
 \item{\dots}{ arguments passed to newConnection.MySQLManager.}
 \item{dbname}{ name of database to connect.}
 \item{username}{ username to use for connection.}
 \item{password}{ MySQL password for the user on the given host.}
 \item{host}{ host to connect to.}
 \item{unix.socket}{ filename for the unix socket.}
 \item{port}{ port number to connect to.}
 \item{client.flag}{ see C API documentation for \code{mysql_real_connect}.}
}
\value{
    dbConnect.MySQLManager returns an object of class MySQLConnection
    inheriting from dbConnection. It holds a single component, Id, of
    class dbObjectId.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{MySQL}}, \code{\link{close}}, \code{\link{getTables}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
getTables(con)
close(con)
}
\keyword{interface}
