\name{getTables}
\alias{getTables}
\title{List All Tables in a Database}
\description{
    List all tables in the specified database. If dbname is missing,
    show tables in the default database for the connection.
}
\usage{
getTables(object, dbname)
}
\arguments{
 \item{object}{ object of class MySQLConnection}
 \item{dbname}{ character vector of length one}
}
\value{
    List of single component with table names in the database
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{dbConnect}}, \code{\link{getDatabases}}}
\examples{
m <- MySQL()
con <- dbConnect(m, user = "nobody")
getTables(con, dbname = "test")
close(con)
con <- dbConnect(m, user = "nobody", dbname = "test")
getTables(con)
close(con)
}
\keyword{interface}
