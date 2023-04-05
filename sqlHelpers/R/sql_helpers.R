
#' Connect to a database using a connection string via DBI/odbc.
#'
#' @param con_str A database connection string.
#' @return A database connection.
#' @examples
#' connect(NULL)
connect <- function(
    con_str = "Driver={PostgreSQL ANSI};Host=localhost;Port=5432;Database=postgres;"
){
  if(is.null(con_str)){
    return(
      warning("con_str should not be NULL")
    )
  }else{
    return(
      odbc::dbConnect(
        odbc::odbc(),
        .connection_string = con_str
      )
    )
  }
}

#' Get the equivalent SQL data type for a given R object.
#'
#' @param x A R object.
#' @param dialect A string, "T-SQL" or "Postgresql".
#' @return A string, the equivalent SQL data type for x.
#' @examples
#' sqlizeTypes(100.1209)
sqlizeTypes <- function(
    x,
    dialect = "T-SQL"
){
  if(dialect == "T-SQL"){
    if (is.factor(x)) return("VARCHAR(255)")
    if (inherits(x, "POSIXt")) return("DATETIME2")
    if (inherits(x, "Date")) return("DATE")
    if (inherits(x, "difftime")) return("TIME2")
    if (inherits(x, "integer64")) return("BIGINT")
    switch(
      typeof(x),
      integer = "BIGINT",
      double = "FLOAT",
      character = "VARCHAR(255)",
      logical = "BIT",
      stop("Unsupported type", call. = FALSE)
    )
  }else if(dialect == "Postgresql"){
    if (is.factor(x)) return("TEXT")
    if (inherits(x, "POSIXt")) return("TIMESTAMP WITH TIME ZONE")
    if (inherits(x, "Date")) return("DATE")
    if (inherits(x, "difftime")) return("TIME")
    if (inherits(x, "integer64")) return("BIGINT")
    switch(
      typeof(x),
      integer = "BIGINT",
      double = "DOUBLE PRECISION",
      character = "TEXT",
      logical = "BOOLEAN",
      stop("Unsupported type", call. = FALSE)
    )
  }
}

#' Convert a column name into a SQL compatible name.
#'
#' @param x A string, a column name.
#' @param dialect A string, "T-SQL" or "Postgresql".
#' @return A string, a SQL compatible column name.
#' @examples
#' sqlizeNames("column 100 - sample b")
sqlizeNames <- function(
    x,
    dialect = "T-SQL"
){
  return(
    if(dialect == "T-SQL"){
      paste0("[", x, "]")
    }else if(dialect == "Postgresql"){
      toolbox::doubleQuoteText(x, char_only = FALSE)
    }
  )
}

#' Retrieve the tables in a schema
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @return A data.table.
#' @examples
#' fetch_tables(con = NULL)
fetch_tables <- function(
    con,
    schema
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    sql <- DBI::sqlInterpolate(
      DBI::ANSI(),
      "SELECT table_schema, table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema = ?schema",
      schema = schema
    )
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Retrieve the columns/types in a table.
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @param table A string, the table to query.
#' @return A data.table.
#' @examples
#' fetch_columns(con = NULL)
fetch_columns <- function(
    con,
    schema,
    table
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    sql <- DBI::sqlInterpolate(
      DBI::ANSI(),
      "SELECT
cols.table_catalog,
cols.table_schema,
cols.table_name,
cols.ordinal_position,
cols.column_name,
CASE
WHEN cols.character_maximum_length = -1
THEN CONCAT(cols.data_type, '(max)')
WHEN cols.character_maximum_length IS NOT NULL
THEN CONCAT(cols.data_type, '(', cols.character_maximum_length, ')')
ELSE cols.data_type
END data_type
FROM
INFORMATION_SCHEMA.columns cols
WHERE
cols.table_schema = ?schema
AND cols.table_name = ?table
ORDER BY
cols.ordinal_position",
      schema = schema,
      table = table
    )
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Retrieve the definition of a function/procedure.
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @param function_name A string, the function/procedure to query.
#' @param type A string, "FUNCTION" or "PROCEDURE".
#' @return A data.table.
#' @examples
#' fetch_function_definition(con = NULL)
fetch_function_definition <- function(
    con,
    schema,
    function_name,
    type = "FUNCTION"
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    sql <- DBI::sqlInterpolate(
      DBI::ANSI(),
      "SELECT
routine_schema schema_name,
routine_name function_name,
specific_name specific_function_name,
routine_definition function_definition
FROM
INFORMATION_SCHEMA.routines
WHERE
routine_type = ?type
AND routine_schema = ?schema
AND routine_name = ?function_name",
      type = type,
      schema = schema,
      function_name = function_name
    )
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Retrieve the input parameters of a function/procedure.
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @param function_name A string, the function/procedure to query.
#' @param type A string, "FUNCTION" or "PROCEDURE".
#' @return A data.table.
#' @examples
#' fetch_function_parameters(con = NULL)
fetch_function_parameters <- function(
    con,
    schema,
    function_name,
    type = "FUNCTION"
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    sql <- DBI::sqlInterpolate(
      DBI::ANSI(),
      "SELECT
funcs.routine_schema schema_name,
funcs.routine_name function_name,
funcs.specific_name specific_function_name,
parameters.parameter_name,
CASE
WHEN parameters.character_maximum_length = -1
THEN CONCAT(parameters.data_type, '(max)')
WHEN parameters.character_maximum_length IS NOT NULL
THEN CONCAT(parameters.data_type, '(', parameters.character_maximum_length, ')')
ELSE parameters.data_type
END data_type,
parameters.ordinal_position
FROM
INFORMATION_SCHEMA.routines funcs
INNER JOIN
INFORMATION_SCHEMA.parameters parameters
ON
funcs.specific_catalog = parameters.specific_catalog
AND funcs.specific_schema = parameters.specific_schema
AND funcs.specific_name = parameters.specific_name
WHERE
parameters.parameter_mode = 'IN'
AND funcs.routine_type = ?type
AND funcs.routine_schema = ?schema
AND funcs.routine_name = ?function_name
ORDER BY
parameters.ordinal_position
",
      type = type,
      schema = schema,
      function_name = function_name
    )
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Retrieve the output parameters of a function/procedure.
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @param function_name A string, the function/procedure to query.
#' @param type A string, "FUNCTION" or "PROCEDURE".
#' @return A data.table.
#' @examples
#' fetch_function_output_parameters(con = NULL)
fetch_function_output_parameters <- function(
    con,
    schema,
    function_name,
    type = "FUNCTION"
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    sql <- DBI::sqlInterpolate(
      DBI::ANSI(),
      "SELECT
funcs.routine_schema schema_name,
funcs.routine_name function_name,
funcs.specific_name specific_function_name,
parameters.parameter_name,
CASE
WHEN parameters.character_maximum_length = -1
THEN CONCAT(parameters.data_type, '(max)')
WHEN parameters.character_maximum_length IS NOT NULL
THEN CONCAT(parameters.data_type, '(', parameters.character_maximum_length, ')')
ELSE parameters.data_type
END data_type,
parameters.ordinal_position
FROM
INFORMATION_SCHEMA.routines funcs
INNER JOIN
INFORMATION_SCHEMA.parameters parameters
ON
funcs.specific_catalog = parameters.specific_catalog
AND funcs.specific_schema = parameters.specific_schema
AND funcs.specific_name = parameters.specific_name
WHERE
parameters.parameter_mode = 'OUT'
AND funcs.routine_type = ?type
AND funcs.routine_schema = ?schema
AND funcs.routine_name = ?function_name
ORDER BY
parameters.ordinal_position
",
      type = type,
      schema = schema,
      function_name = function_name
    )
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Call a SQL function/procedure.
#'
#' @param con A database connection.
#' @param schema A string, the schema to query.
#' @param function_name A string, the function/procedure to query.
#' @param args A named list or vector, names are the parameter names and values are the parameter values.
#' @param dialect A string, "T-SQL" or "Postgresql".,
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the parameters to the specified type.
#' @return A data.table.
#' @examples
#' call_function(con = NULL)
call_function <- function(
    con,
    schema,
    function_name,
    args,
    dialect = "T-SQL",
    cast = TRUE
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    args <- unlist(args)
    if(length(args) > 0){
      if(dialect == "T-SQL"){
        names(args) <- paste0("@", names(args))
      }
    }
    x <- fetch_function_parameters(con, schema = schema, function_name = function_name)
    null_args <- x[["parameter_name"]][!(x[["parameter_name"]] %in% names(args))]
    nulls <- rep_len("NULL", length(null_args))
    if(length(args) > 0){
      args <- args[names(args) %in% x[["parameter_name"]]]
      arg_names <- names(args)
      if(cast){
        types <- unlist(lapply(arg_names, function(y){
          x[["data_type"]][x[["parameter_name"]] == y]
        }), use.names = FALSE)
        args <- paste0("CAST(", toolbox::quoteText(args), " AS ", types, ")")
      }else{
        args <- toolbox::quoteText(args)
      }
      args <- c(args, nulls)
      arg_names <- c(arg_names, null_args)
      args <- paste0(arg_names, " = ", args, collapse = ", ")
    }else if(length(nulls) > 0){
      args <- paste0(null_args, " = ", nulls, collapse = ", ")
    }else{
      args <- ""
    }
    if(dialect == "T-SQL"){
      sql <- paste0("EXEC ", schema, ".", function_name, " ", args, ";")
    }else if(dialect == "Postgresql"){
      sql <- paste0("SELECT * FROM ", schema, ".", function_name, "(", args, ");")
    }
    return(
      as.data.table(
        DBI::dbGetQuery(
          con,
          sql
        )
      )
    )
  }
}

#' Generate a CREATE TABLE statement based on a data.frame, optionally execute the statement if con is not NULL.
#'
#' @param x A data.frame.
#' @param table_name A string, the name of the SQL table to create.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, the CREATE TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' create_table_from_data_frame(x = iris, table_name = "test")
create_table_from_data_frame <- function(
    x,
    table_name,
    con = NULL
){
  cols <- names(x)
  types <- unlist(lapply(x, sqlizeTypes), use.names = FALSE)
  x <- paste0(
    "CREATE TABLE ", table_name, " ", "(\n",
    paste0(cols, " ", types, collapse = ",\n"),
    "\n)",
    ";"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a DROP TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param args A string, the arguments to add to the DROP TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, the DROP TABLE statement; or the results retrieved by DBI::dbSendQuery after executing the statement.
#' @examples
#' drop_table("sample")
drop_table <- function(
    args,
    con = NULL
){
  x <- paste0("DROP TABLE ", args, ";")
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a TRUNCATE TABLE statement, optionally execute the statement if con is not NULL.
#'
#' @param args A string, the arguments to add to the TRUNCATE TABLE statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @return A string, the TRUNCATE TABLE statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' truncate_table(args = "table1")
truncate_table <- function(
    args,
    con = NULL
){
  x <- paste0("TRUNCATE TABLE ", args, ";")
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Add single quotes to strings using stringi::stri_join, useful for converting R strings into SQL formatted strings.
#'
#' @param x A string.
#' @param char_only TRUE/FALSE, if TRUE, adds quotes only if is.character(x) is TRUE.
#' @param excluded_chars A character vector, will not add quotes if a value is in excluded_chars.
#' @return A string, with single quotes added to match SQL string formatting.
#' @examples
#' quoteText2("Sample quotes.")
quoteText2 <- function(
    x,
    char_only = TRUE,
    excluded_chars = c("NULL")
){
  if(char_only == TRUE){
    x[is.character(x) & !(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)] <- stringi::stri_join("'", gsub("\'", "''", x[is.character(x) & !(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)], fixed = TRUE), "'")
  }else{
    x[!(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)] <- stringi::stri_join("'", gsub("\'", "''", x[!(x %in% excluded_chars) & !(is.null(x) == TRUE|is.na(x) == TRUE)], fixed = TRUE), "'")
  }
  x[is.null(x) == TRUE|is.na(x) == TRUE] <- "NULL"
  return(
    x
  )
}

#' Helper function for INSERT
#'
#' @param x A vector of data to insert.
#' @param n_batches Integer, the number of batches needed to insert the data.
#' @param batch_size Integer, the size of each batch.
#' @return A list.
#' @examples
#' insert_batch_chunker(c(1, 2, 3), 1, 100)
insert_batch_chunker <- function(x, n_batches, batch_size){
  if(n_batches == 1){
    return(
      list(x)
    )
  }else{
    i <- seq_len(n_batches)
    starts <- batch_size*i - batch_size + 1
    ends <- batch_size*i
    ends[length(ends)] <- as.character(length(x))
    str <- paste0("list(", paste0(shQuote(i), " = x[", starts, ":", ends,"]", collapse = ", "), ")")
    return(eval(str2expression(str)))
  }
}

#' Generate a INSERT statement, optionally execute the statement if con is not NULL.
#'
#' @param x A list, data.frame or data.table, names must match the column names of the destination SQL table.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param returning A vector of character strings specifying the SQL column names to be returned by the INSERT statement.
#' @param quote_text TRUE/FALSE, if TRUE, calls quoteText() to add single quotes around character strings.
#' @param cast TRUE/FALSE, if TRUE, will add SQL to cast the data to be inserted to the specified type.
#' @param types A vector of types to use for casting columns. If blank, will look at meta data about table to decide types.
#' @param batch_size Integer, the maximum number of records to submit in one statement.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @param n_cores A integer, the number of cores to use for parallel forking (passed to parallel::mclapply as mc.cores).
#' @param table_is_temporary TRUE/FALSE, if TRUE, prevents parallel processing.
#' @param retain_insert_order TRUE/FALSE, if TRUE, prevents parallel processing.
#' @param connect_db_name The name of the database to pass to connect() when inserting in parallel.
#' @param dialect A string, "T-SQL" or "Postgresql".
#' @return A string, the INSERT statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' insert_values(
#' x = list(col1 = c("a", "b", "c"), col2 = c(1, 2, 3)),
#' schema = "test",
#' table = "table1",
#' types = c("VARCHAR(12)", "INT")
#' )
insert_values <- function(
    x = NULL,
    schema = NULL,
    table,
    returning = NULL,
    quote_text = TRUE,
    cast = TRUE,
    types = NULL,
    batch_size = 1000,
    con = NULL,
    table_is_temporary = FALSE,
    retain_insert_order = FALSE,
    n_cores = 1,
    connect_db_name = NULL,
    dialect = "T-SQL"
){
  x <- as.list(x)
  x_names <- names(x)
  if(!is.null(con) & is.null(types) & cast){
    cols <- fetch_columns(con, schema, table)
    types <- unlist(lapply(x_names, function(y){
      cols[["data_type"]][cols[["column_name"]] == y]
    }), use.names = FALSE)
  }
  n_cols <- length(x_names)
  i <- seq_len(n_cols)
  n_rows <- length(x[[1]])
  n_batches <- ceiling(n_rows/batch_size)

  parenthesis <- rep_len('"),"', n_cols)
  parenthesis[n_cols] <- '")"'
  if(quote_text & cast){
    str <- stringi::stri_join(
      'stringi::stri_join("(",', stringi::stri_join('"CAST(", quoteText2(x[[', i, ']]), " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ', ")")'
    )
  }else if(cast){
    str <- stringi::stri_join(
      'stringi::stri_join("(",', stringi::stri_join('"CAST(", x[[', i, ']], " AS ", types[[', i, ']], ', parenthesis, collapse = ","), ',")")'
    )
  }else if(quote_text){
    str <- stringi::stri_join(
      'stringi::stri_join("(",', stringi::stri_join('quoteText2(x[[', i, ']])', collapse = ", ',', "), ', ")")'
    )
  }else{
    str <- stringi::stri_join(
      'stringi::stri_join("(",', stringi::stri_join('x[[', i, ']]', collapse = ", ',', "), ', ")")'
    )
  }
  insert <- eval(str2expression(str))
  rm(x)
  insert_batches <- insert_batch_chunker(insert, n_batches, batch_size)
  if(is.null(con)){
    return(
      unlist(lapply(insert_batches, function(i){
        return(
          stringi::stri_join(
            "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
            if(dialect == "T-SQL"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nOUTPUT ", stringi::stri_join("INSERTED.", returning, collapse = ", "))}
            }else{
              ""
            },
            " VALUES \n", stringi::stri_join(i, collapse = ",\n"),
            if(dialect == "Postgresql"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(returning, collapse = ", "))}
            }else{
              ""
            },
            ";"
          )
        )
      }), use.names = FALSE)
    )
  }else{
    if(n_cores > 1 & !table_is_temporary & !retain_insert_order){
      parallel::mclapply(insert_batches, function(i){
        con <- connect(connect_db_name)
        DBI::dbGetQuery(
          conn = con,
          statement = stringi::stri_join(
            "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
            if(dialect == "T-SQL"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nOUTPUT ", stringi::stri_join("INSERTED.", returning, collapse = ", "))}
            }else{
              ""
            },
            " VALUES \n", stringi::stri_join(i, collapse = ",\n"),
            if(dialect == "Postgresql"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(returning, collapse = ", "))}
            }else{
              ""
            },
            ";"
          )
        )
        DBI::dbDisconnect(con)
        return()
      },
      mc.cores = n_cores)
    }else{
      lapply(insert_batches, function(i){
        DBI::dbGetQuery(
          conn = con,
          statement = stringi::stri_join(
            "INSERT INTO ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, " (", stringi::stri_join(x_names, collapse = ", "), ")",
            if(dialect == "T-SQL"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nOUTPUT ", stringi::stri_join("INSERTED.", returning, collapse = ", "))}
            }else{
              ""
            },
            " VALUES \n", stringi::stri_join(i, collapse = ",\n"),
            if(dialect == "Postgresql"){
              if(is.null(returning)){""}else{stringi::stri_join(" \nRETURNING ", stringi::stri_join(returning, collapse = ", "))}
            }else{
              ""
            },
            ";"
          )
        )
        return()
      })
    }
  }
}

#' Generate a BULK INSERT statement, optionally execute the statement if con is not NULL.
#'
#' @param file A string, the file path to the file with data to insert.
#' @param schema A string, the schema name of the destination SQL table.
#' @param table A string, the table name of the destination SQL table.
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @param ... named arguments are passed to the WITH statement.
#' @return A string, the BULK INSERT statement; or the results retrieved by DBI::dbGetQuery after executing the statement.
#' @examples
#' t_sql_bulk_insert(
#' file = tempfile(),
#' schema = "test",
#' table = "table1",
#' format = 'CSV',
#' first_row = 2,
#' )
t_sql_bulk_insert <- function(
    file,
    schema = NULL,
    table,
    con = NULL,
    ...
){
  args <- c(...)
  args <- paste0(names(args), " = ", toolbox::quoteText(unname(args)), collapse = ", ")
  x <- paste0(
    "BULK INSERT ", if(is.null(schema)){table}else{paste(schema, table, sep = ".")}, "\n",
    "FROM ", toolbox::quoteText(file), "\n",
    "WITH (", args, ");"
  )
  if(is.null(con) == TRUE){
    return(x)
  }else{
    x <- DBI::dbSendQuery(con, x)
    DBI::dbClearResult(x)
    return(x)
  }
}

#' Generate a CREATE TABLE statement for an existing table in Microsoft SQL Server.
#'
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @param table A string, the schema qualified table name of an existing SQL table.
#' @return A data table, contains the DDL scripts for creating a table.
#' @examples
#' t_sql_script_create_table(con = NULL)
t_sql_script_create_table <- function(
    con,
    table
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    x <- as.list(DBI::dbGetQuery(
      con,
      DBI::sqlInterpolate(
        DBI::ANSI(),
        "WITH
oid AS (
SELECT
      obj_name = '[' + s.name + '].[' + o.name + ']'
    , obj_id = o.[object_id]
FROM sys.objects o WITH (NOWAIT)
JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]
WHERE s.name + '.' + o.name = ?table
    AND o.[type] = 'U'
    AND o.is_ms_shipped = 0
),
index_column AS
(
    SELECT
          ic.[object_id]
        , ic.index_id
        , ic.is_descending_key
        , ic.is_included_column
        , c.name
    FROM sys.index_columns ic WITH (NOWAIT)
    JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
    WHERE ic.[object_id] = (SELECT obj_id FROM oid)
),
fk_columns AS
(
     SELECT
          k.constraint_object_id
        , cname = c.name
        , rcname = rc.name
    FROM sys.foreign_key_columns k WITH (NOWAIT)
    JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id
    JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id
    WHERE k.parent_object_id = (SELECT obj_id FROM oid)
)
SELECT
[SQL] = 'CREATE TABLE ' + (SELECT obj_name FROM oid) + CHAR(13) + '(' + CHAR(13) + STUFF((
    SELECT CHAR(9) + ', [' + c.name + '] ' +
        CASE WHEN c.is_computed = 1
            THEN 'AS ' + cc.[definition]
            ELSE UPPER(tp.name) +
                CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                     WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                     WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                       THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                     WHEN tp.name = 'decimal'
                       THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                    ELSE ''
                END +
                CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END +
                CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END
        END + CHAR(13)
    FROM sys.columns c WITH (NOWAIT)
    JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
    LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
    LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
    LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
    WHERE c.[object_id] = (SELECT obj_id FROM oid)
    ORDER BY c.column_id
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
    + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
                    (SELECT STUFF((
                         SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                         FROM sys.index_columns ic WITH (NOWAIT)
                         JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                         WHERE ic.is_included_column = 0
                             AND ic.[object_id] = k.parent_object_id
                             AND ic.index_id = k.unique_index_id
                         FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
            + ')' + CHAR(13)
            FROM sys.key_constraints k WITH (NOWAIT)
            WHERE k.parent_object_id = (SELECT obj_id FROM oid)
                AND k.[type] = 'PK'), '') + ')' + ';' + CHAR(13)
    + ISNULL((SELECT (
        SELECT CHAR(13) +
             'ALTER TABLE ' + (SELECT obj_name FROM oid) + ' WITH'
            + CASE WHEN fk.is_not_trusted = 1
                THEN ' NOCHECK'
                ELSE ' CHECK'
              END +
              ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('
              + STUFF((
                SELECT ', [' + k.cname + ']'
                FROM fk_columns k
                WHERE k.constraint_object_id = fk.[object_id]
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
               + ')' +
              ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
              + STUFF((
                SELECT ', [' + k.rcname + ']'
                FROM fk_columns k
                WHERE k.constraint_object_id = fk.[object_id]
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
               + ')'
            + CASE
                WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
                WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
                ELSE ''
              END
            + CASE
                WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
                ELSE ''
              END
             + ';' + CHAR(13) + 'ALTER TABLE ' + (SELECT obj_name FROM oid) + ' CHECK CONSTRAINT [' + fk.name  + ']' + ';' + CHAR(13)
        FROM sys.foreign_keys fk WITH (NOWAIT)
        JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
        WHERE fk.parent_object_id = (SELECT obj_id FROM oid)
        FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')
    + ISNULL(((SELECT
         CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
                + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + (SELECT obj_name FROM oid) + ' (' +
                STUFF((
                SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                FROM index_column c
                WHERE c.is_included_column = 0
                    AND c.index_id = i.index_id
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
                + ISNULL(CHAR(13) + 'INCLUDE (' +
                    STUFF((
                    SELECT ', [' + c.name + ']'
                    FROM index_column c
                    WHERE c.is_included_column = 1
                        AND c.index_id = i.index_id
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '') + ';' + CHAR(13)
        FROM sys.indexes i WITH (NOWAIT)
        WHERE i.[object_id] = (SELECT obj_id FROM oid)
            AND i.is_primary_key = 0
            AND i.[type] = 2
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    ), ''),
[CREATE] = 'CREATE TABLE ' + (SELECT obj_name FROM oid) + CHAR(13) + '(' + CHAR(13) + STUFF((
    SELECT CHAR(9) + ', [' + c.name + '] ' +
        CASE WHEN c.is_computed = 1
            THEN 'AS ' + cc.[definition]
            ELSE UPPER(tp.name) +
                CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                     WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')
                       THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                     WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                       THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                     WHEN tp.name = 'decimal'
                       THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
                    ELSE ''
                END +
                CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +
                CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +
                CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END +
                CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END
        END + CHAR(13)
    FROM sys.columns c WITH (NOWAIT)
    JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
    LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id
    LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
    LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
    WHERE c.[object_id] = (SELECT obj_id FROM oid)
    ORDER BY c.column_id
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')
    + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +
                    (SELECT STUFF((
                         SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END
                         FROM sys.index_columns ic WITH (NOWAIT)
                         JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id
                         WHERE ic.is_included_column = 0
                             AND ic.[object_id] = k.parent_object_id
                             AND ic.index_id = k.unique_index_id
                         FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))
            + ')' + CHAR(13)
            FROM sys.key_constraints k WITH (NOWAIT)
            WHERE k.parent_object_id = (SELECT obj_id FROM oid)
                AND k.[type] = 'PK'), '') + ')' + ';' + CHAR(13),
[CONSTRAINT] = ISNULL((SELECT (
        SELECT 'ALTER TABLE ' + (SELECT obj_name FROM oid) + ' WITH'
            + CASE WHEN fk.is_not_trusted = 1
                THEN ' NOCHECK'
                ELSE ' CHECK'
              END +
              ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('
              + STUFF((
                SELECT ', [' + k.cname + ']'
                FROM fk_columns k
                WHERE k.constraint_object_id = fk.[object_id]
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
               + ')' +
              ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('
              + STUFF((
                SELECT ', [' + k.rcname + ']'
                FROM fk_columns k
                WHERE k.constraint_object_id = fk.[object_id]
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')
               + ')'
            + CASE
                WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE'
                WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'
                WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'
                ELSE ''
              END
            + CASE
                WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'
                WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'
                WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT'
                ELSE ''
              END
             + ';' + CHAR(13) + 'ALTER TABLE ' + (SELECT obj_name FROM oid) + ' CHECK CONSTRAINT [' + fk.name  + ']' + ';' + CHAR(13)
        FROM sys.foreign_keys fk WITH (NOWAIT)
        JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id
        WHERE fk.parent_object_id = (SELECT obj_id FROM oid)
        FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), ''),
[INDEX] = ISNULL(((SELECT
         'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END
                + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + (SELECT obj_name FROM oid) + ' (' +
                STUFF((
                SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                FROM index_column c
                WHERE c.is_included_column = 0
                    AND c.index_id = i.index_id
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'
                + ISNULL(CHAR(13) + 'INCLUDE (' +
                    STUFF((
                    SELECT ', [' + c.name + ']'
                    FROM index_column c
                    WHERE c.is_included_column = 1
                        AND c.index_id = i.index_id
                    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '') + ';' + CHAR(13)
        FROM sys.indexes i WITH (NOWAIT)
        WHERE i.[object_id] = (SELECT obj_id FROM oid)
            AND i.is_primary_key = 0
            AND i.[type] = 2
        FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
    ), '')",
    table = table
      )
    ))
    x <- lapply(x, function(y){
      gsub("\r", "\n", y)
    })
    return(as.data.table(x))
  }
}

#' Fetch the object definition of a proc in Microsoft SQL Server.
#'
#' @param con A database connection that can be passed to DBI::dbSendQuery/DBI::dbGetQuery.
#' @param proc A string, the database and schema qualified table name of an existing SQL stored procedure.
#' @return A string, contains the script for defining a stored procedure.
#' @examples
#' t_sql_script_proc_definition(con = NULL)
t_sql_script_proc_definition <- function(
    con,
    proc
){
  if(is.null(con)){
    return(
      warning("con should not be NULL")
    )
  }else{
    x <- DBI::dbGetQuery(
      con,
      DBI::sqlInterpolate(
        DBI::ANSI(),
        "SELECT OBJECT_DEFINITION (OBJECT_ID(N?proc))",
        proc = proc
      )
    )[[1]]
    return(x)
  }
}
