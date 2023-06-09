% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql_helpers.R
\name{t_sql_script_proc_definition}
\alias{t_sql_script_proc_definition}
\title{Fetch the object definition of a proc in Microsoft SQL Server.}
\usage{
t_sql_script_proc_definition(con, proc)
}
\arguments{
\item{con}{A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.}

\item{proc}{A string, the database and schema qualified table name of an existing SQL stored procedure.}
}
\value{
A string, contains the script for defining a stored procedure.
}
\description{
Fetch the object definition of a proc in Microsoft SQL Server.
}
\examples{
t_sql_script_proc_definition(con = NULL)
}
