\name{getInfo}
\alias{getInfo}
\title{ Get Information About an SQL Driver object}
\description{
    For a MySQLManger, MySQLconnection or MySQLResultSet object, this
    returns a list of informations.
}
\usage{
getInfo(object)
}
\arguments{
 \item{object}{ object of class MySQLManger, MySQLconnection or MySQLResultSet}
}
\value{
    A list containing fields depending on object class
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
getInfo(m)
getInfo(con)
getInfo(rs)
fetch(rs, n = -1)
getInfo(rs)
close(con)
}
\keyword{interface}
