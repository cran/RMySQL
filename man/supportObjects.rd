\name{supportObjects}
\alias{.MySQL.NA.string}
\alias{.SQL92Keywords}
\alias{.MySQLKeywords}
\alias{.conflicts.OK}
\alias{existsFunction}
\non_function{}
\title{Support objects for RS-DBI}
\usage{data(.SQLKeywords)}
\description{
  \code{.SQLKeywords} is a vector of SQL92 reserved words -- it may be
  overriden to include additional identifiers reserved by 
  RDBMS.

  \code{.MySQL.NA.string} is the string that MySQL interprets as
  SQL's \code{NULL}, which is what we used to represent \code{NA}.

}
\keyword{datasets}
\keyword{interface}
\keyword{database}
\keyword{internal}
% vim: syntax=tex
