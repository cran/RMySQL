\name{MySQL}
\alias{MySQL}
\alias{MySQLManager}
\title{
  Instantiate a MySQL client from the current R or S session
}
\description{
This function creates and initializes a MySQL client.
It returns an object that allows you to connect
to one or several MySQL servers.
}
\usage{
MySQL(max.con = 10, fetch.default.rec = 500, force.reload = F)
}
\arguments{
\item{max.con }{
  maximum number of connections that we intended to have open.
  This can be up to 100, a limit defined at compilation time.
  Strictly speaking this limit applies to MySQL \emph{servers},
  but clients can have (at least in theory) more than this.
  }
\item{fetch.default.rec}{
  number of records to fetch at one time from the database.
  (The \code{fetch} method uses this number as a default.)
  }
\item{force.reload}{
  should we reload (reinitialize) the client code?
  Setting this to \code{TRUE} allows you to change
  default settings.  Notice that all connections should be closed
  before re-loading.
  }
}
\value{
An object \code{MySQLManager} that extends 
\code{dbManager} and 
\code{dbObjectId}.
This object is required to create connections
to one or several MySQL database engines.
}
\section{Side Effects}{
The R/S client part of the database communication is initialized,
but note that connecting to the database engine needs to be done through
calls to \code{dbConnect}.
}
\details{
This object is a singleton, that is, on subsequent invocations
it returns the same initialized object. 

This implementation allows you to connect
to multiple host servers and run multiple connections on each server
simultaneously.
}
\section{User authentication}{
The preferred method to pass authentication parameters to the
server (e.g., user, password, host) is through the MySQL configuration
file \file{\$HOME/.my.cnf}.
Since specifying passwords on calls to 
\code{dbConnect}
is a very bad idea (and so is specifying passwords through
shell variables),
the client code parses the configuration file \file{\$HOME/.my.cnf};
this file consists of zero or more sections, each starting with a 
line of the form \code{[section-name]}, for instance

\$ cat \$HOME/.my.cnf\cr
\# this is a comment\cr
[client]\cr
user = dj\cr
host = localhost\cr
\cr
[rs-dbi]\cr
database = s-data\cr
\cr
[lasers]\cr
user = opto\cr
database = opto\cr
password = pure-light\cr
host = merced\cr
...\cr
[iptraffic]\cr
host = data\cr
database = iptraffic\cr
\cr
This file should be readeable only by you. 
Inside each section, MySQL parameters may be specified one per line
(e.g., \code{user = opto}).  
The R/S MySQL implementation always parses the \code{[client]}
and \code{[rs-dbi]} sections, but you may define you own
project-specific sections to tailor its environment; 
if the same parameter appears more than once, the last
(closer to the bottom) occurrence is used.

If you define a section, say, \code{[iptraffic]},
then instead of including all these parameters in the
call to \code{dbConnect}, you simply supply the
name of the \code{group},
e.g., \code{dbConnect(mgr, group = "iptraffic")}.

The most important parameters are \code{user}, 
\code{password}, \code{host}, 
and \code{dbname}.
}
\notes{
The \code{dbname} cannot go in the
\code{[client]} section, but you may safely include 
it under the \code{[rs-dbi]} section or one you define
yourself.
}
\section{References}{
See the Omega Project for Statistical Computing
at \url{http://www.omegahat.org}
for more details on the R/S database interface.

See the documentation at the MySQL Web site
\url{http://www.mysql.com} for details.
}
\seealso{
On database managers:

\code{\link{dbManager}}
\code{\link{MySQL}}
\code{\link{load}}
\code{\link{unload}}

On connections, SQL statements and resultSets:

\code{\link{dbExecStatement}}
\code{\link{dbExec}}
\code{\link{fetch}}
\code{\link{quickSQL}}

On transaction management:

\code{\link{commit}}
\code{\link{rollback}}

On meta-data:

\code{\link{describe}}
\code{\link{getVersion}}
\code{\link{getDatabases}}
\code{\link{getTables}}
\code{\link{getFields}}
\code{\link{getCurrentDatabase}}
\code{\link{getTableIndices}}
\code{\link{getException}}
\code{\link{getStatement}}
\code{\link{hasCompleted}}
\code{\link{getRowCount}}
\code{\link{getAffectedRows}}
\code{\link{getNullOk}}
\code{\link{getInfo}}
}
\examples{\dontrun{
# create a MySQL instance and create one connection.
> m <- dbManager("MySQL")
> m
MySQLManager id = (7269) 

# open the connection using user, passsword, etc., as
# specified in the "[iptraffic]" section of the 
# configuration file \file{\$HOME/.my.cnf}
> con <- dbConnect(m, group = "iptraffic")
> rs <- dbExec(con, "select * from HTTP_ACCESS where IP_ADDRESS = '127.0.0.1'")
> df <- fetch(rs, n = 50)
> hasCompleted(rs)
[1] FALSE
> df2 <- fetch(rs, n = -1)
> hasCompleted(rs)
[1] TRUE
> close(rs)
> dim(quickSQL(con, "show tables"))
[1] 74   1
> getTables(con)    
}
}
\keyword{interface}
\keyword{database}
% docclass is function
% Converted by Sd2Rd version 1.15.2.1.
% vim: syntax=tex
