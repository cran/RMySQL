\name{isIdCurrent}
\alias{isIdCurrent}
\title{ Verify a dbObjectId Object}
\description{
    This function verifies that a dbObjectId refers to a currently
    open/loaded database.
}
\usage{
isIdCurrent(object)
}
\arguments{
 \item{object}{ an object of class dbObjectId or one that has a single
     dbObjectId component called Id}
}
\value{
    TRUE or FALSE
}
\references{\url{http://www.mysql.com/}}
\author{David James}
\seealso{ \code{\link{MySQL}}}
\examples{
m <- MySQL()
isIdCurrent(m)
}
\keyword{interface}
