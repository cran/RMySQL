\name{getFields}
\alias{getFields}
\title{ Get Information About the Fields in a resultSet}
\description{
    For a MySQLResultSet object, this returns a list of its fields.
}
\usage{
getFields(object)
}
\arguments{
 \item{object}{ object of class MySQLResultSet}
}
\value{
    A data.frame describing the fields.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
getFields(rs)
close(rs)
close(con)
}
\keyword{interface}
