\name{fetch}
\alias{fetch}
\title{ Fetch Rows From an SQL resultSet}
\description{
    Fetch \code{n} rows from the resultSet. If \code{n} is missing, a
    default number of rows are fetched. If \code{n} is -1, all rows are
    fetched.
}
\usage{
fetch(resultSet, n)
}
\arguments{
 \item{resultSet}{ object of class MySQLResultSet }
 \item{n}{ integer - number of rows to fetch }
}
\value{
    A list of columns in the result.
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{dbExecStatement}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody", dbname = "test")
rs <- dbExecStatement(con, "SHOW TABLES")
fetch(rs, n = -1)
close(con)
}
\keyword{interface}
