\name{close.MySQLResultSet}
\alias{close.MySQLResultSet}
\title{ Close a MySQL Result} 
\description{
    Close a MySQLResultSet object. If a query has pending rows, this
    must be called before starting a new query.
}
\usage{
close.resultSet(con)
}
\arguments{
 \item{con}{ an object of class MySQLResultSet }
}
\value{
    TRUE if the result was successfully closed.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{MySQLManager}}, \code{\link{dbConnect}},
    \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW DATABASES")
\dontrun{
rs <- dbExecStatement(con, "SHOW TABLES")  # error
}
close(rs)
rs <- dbExecStatement(con, "SHOW TABLES")
fetch(rs, n = -1)                          # fetch all rows
close(rs)
rs <- dbExecStatement(con, "SHOW DATABASES")  # no problem here
fetch(rs, n = -1)
close(rs)
close(con)
}
\keyword{interface}
