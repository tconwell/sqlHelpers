% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql_helpers.R
\name{fetch_function_parameters}
\alias{fetch_function_parameters}
\title{Retrieve the input parameters of a function/procedure.}
\usage{
fetch_function_parameters(con, schema, function_name, type = "FUNCTION")
}
\arguments{
\item{con}{A database connection.}

\item{schema}{A string, the schema to query.}

\item{function_name}{A string, the function/procedure to query.}

\item{type}{A string, "FUNCTION" or "PROCEDURE".}
}
\value{
A data.table.
}
\description{
Retrieve the input parameters of a function/procedure.
}
\examples{
fetch_function_parameters(con = NULL)
}
