\name{hasCompleted}
\alias{hasCompleted}
\title{ Check a resultSet for Unfetched Rows }
\description{
    This checks if all rows have been fetched for a resultSet object.
}
\usage{
hasCompleted(object)
}
\arguments{
 \item{object}{ object of class resultSet }
}
\value{
    TRUE or FALSE
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
hasCompleted(rs)
fetch(rs, n = -1)
hasCompleted(rs)
close(rs)
close(con)
}
\keyword{interface}
