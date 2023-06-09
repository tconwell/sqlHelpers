% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sql_helpers.R
\name{sqlizeNames}
\alias{sqlizeNames}
\title{Convert a column name into a SQL compatible name.}
\usage{
sqlizeNames(x, dialect = "T-SQL")
}
\arguments{
\item{x}{A string, a column name.}

\item{dialect}{A string, "T-SQL" or "Postgresql".}
}
\value{
A string, a SQL compatible column name.
}
\description{
Convert a column name into a SQL compatible name.
}
\examples{
sqlizeNames("column 100 - sample b")
}
