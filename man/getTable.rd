\name{getTable}
\alias{getTable}
\alias{assignTable}
\alias{existsTable}
\alias{removeTable}
\title{
  Convenience functions for Importing/Exporting RDBMS tables into R/S.
}
\description{
These functions mimic their R/S counterpart 
\code{get},
\code{assign},
\code{exists}, and
\code{remove},
except that they generate SQL code that gets executed
in a relational database engine.
They all work over a 
\code{RDBMS connection},
and an SQL table name.
}
\usage{
getTable(con, name, ...)
assignTable(con, name, value, ..., overwrite = F, append = F)
existsTable(con, name, ...)
removeTable(con, name, ...)
}
\arguments{
\item{con}{
a database connection object.
}
\item{name}{
a character string specifying the SQL table name.
}
\item{value}{
an R/S data.frame (or coerceable to data.frame).
}
\item{overwrite}{
a logical specifying whether to overwrite an existing table
or not.  Its default is \code{FALSE}.
}
\item{append}{
a logical specifying whether to append to an existing table
in the RDBMS.
Its default is \code{FALSE}.
}
\item{\dots }{
any optional arguments that the underlying database driver
supports. 
}
}
\value{
\code{getTable} returns a data.frame;
the other functions return \code{TRUE} or
\code{FALSE} denoting whether the operation
was successful or not.
}
\section{Side Effects}{
An SQL statement is generated and executed on the RDBMS;
the result set it produces is fetched in its entirety.
These operations may failed if the underlying database driver
runs out of available connections and/or result sets.
}
\section{References}{
See the The Omega Project for Statistical Computing 
\url{http://www.omegahat.org} for details on the R/S database interface.
}
\seealso{
\code{\link{get}}
\code{\link{assign}}
\code{\link{exists}}
\code{\link{remove}}
\code{\link{dbConnect}}
\code{\link{MySQL}}
}
\examples{\dontrun{
con <- dbConnect("MySQL", group = "vitalAnalysis")
assignTable(con, "fuel_frame", fuel.frame)
removeTable(con, "fuel_frame")
if(existsTable(con, "RESULTS")){
   assignTable(con, "RESULTS", results2000, append = T)
else
   assignTable(con, "RESULTS", results2000)
}
\keyword{data}
\keyword{manip }
\keyword{interface }
\keyword{programming }
\keyword{enviroment }
\keyword{databases}
\keyword{RDBMS }
\keyword{RS-DBI}
\keyword{MySQL }
% docclass is function
% Converted by Sd2Rd version 1.15.2.1.
