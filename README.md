# go-mssql-load - load sql scripts and test data into MSSQL databases

This is a small tool that serves two main purposes

- help with populating a MSSQL db with schema and test data during integration testing.
  This can be on your local machine or part of a CI/CD pipeline.
- provide a simple way for (data) pipelines to set up the schema of a db where the data
  is exported to.

## Use Cases

### Integration Testing

The [sqlcmd utility](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility)
is usually the go-to solution for interacting with MSSQL databases. However, it can be
difficult to install and use, especially in the context of Apple M1 machines. That can
pose a problem to developers, because now two different ways of dealing with the
database exist. One in the CI/CD pipeline, that uses sqlcmd and one on the developer's
local machine.

To make the DB setup process consistent, `go-mssql-load` can be used.

### Set up a DB schema

Every Java developer will probably scream [FLYWAY](https://flywaydb.org/) or similar,
but let's assume that your group is more interdisciplinary, consisting of devs and data
engineers. Having a simple CLI tool to setup up the DB can help with shared ownership of
the schema.

### Bulk load test data

You can bulk load CSV data. This allows you to populate a test db with some or many
rows. In particular for data engineering purposes it is a good way to load a bigger
chunk of data for a more _realistic_ feeling.

## Installation

You need to have go installed.

To install it directly:

```console
go get github.com/jwbargsten/go-mssql-load
```

If you want to install it from the repo, you can run

```console
$ go build
```

which will create the executable `./go-mssql-load` or install it into `$GOPATH/bin`
using

```console
$ go install
```

## Usage

Before you can use `go-mssql-load`, you have to have a mssql db running. The easiest way
would be to use one of the official Microsoft docker containers (azure sql edge is
mostly compatible with mssql and it runs on M1 machines):

```console
$ docker run --rm -p 1433:1433 \
  -e "ACCEPT_EULA=1" \
  -e "MSSQL_SA_PASSWORD=Passw0rd" \
  mcr.microsoft.com/azure-sql-edge:latest
```

### Basic usage

With docker running, you can start playing:

```console
# print the help/usage
$ go-mssql-load help

# print the DSN
$ go-mssql-load printdsn

# check the connection for localhost:1433
$ go-mssql-load --user sa --pass Passw0rd check

# load some example data
$ go-mssql-load --user sa --pass Passw0rd loadsql sql/init.sql

# try a query
$ echo "select * from pokemon.pokemon" | go-mssql-load --user sa --pass Passw0rd querysql -
# try a query, ignore logging
$ echo "select * from pokemon.pokemon" | go-mssql-load --user sa --pass Passw0rd querysql - 2>/dev/null
```

### CSV loading

You can use this tool to do CSV bulk loading. By default all columns are treated as
string, but you can specify a data type as part of the column name or as argument.
`loadcsv` doesn't have any parsing magic and uses the
[csv parser provided by the go std lib](https://pkg.go.dev/encoding/csv). So, if you
don't specify it, it won't happen. The spec is as follows:

```
<name>::<datatype>[!]
```

with an optional `!` indicating a nullable column.

| column spec      | column name | data type | nullable |
| ---------------- | ----------- | --------- | -------- |
| `name::string!`  | `name`      | `string`  | yes      |
| `name::!`        | `name`      | `string`  | yes      |
| `name`           | `name`      | `string`  | no       |
| `age::int!`      | `age`       | `int64`   | yes      |
| `height::float!` | `age`       | `float64` | yes      |
| `height::float`  | `age`       | `float64` | no       |

Internally the golang parse functions are used.

Supported types:

- `int` » `strconv.ParseInt(v, 10, 64)`
- `float` » `strconv.ParseFloat(v, 64)`
- `bool` » custom parsing, anything that looks like: `TRUE`, `true`, `T`, `t`, `YES`,
  `yes`, `Y`, `y`, `1` is considered true.
- `string` as default

Example: `./sql/pokemon_typed.csv`

You can set the null string and the separator via cli flags. The null string is by
default the empty string `""`.

The TAB character is a bit tricky to specify, but you can just supply a quoted TAB to
parse [TSV files](https://en.wikipedia.org/wiki/Tab-separated_values):

```console
$ go-mssql-load --user sa --pass Passw0rd loadcsv --sep "	" pokemon.pokemon sql/pokemon_typed.csv
```

Only columns that have the nullable flag `!` will use the `nullstr` flag.

As mentioned in the beginning, you can also supply an external types file in JSON
format. There are two options supported, as dict or as list.

The dict looks as follows (order is not important):

```
{
  "colname2": "int",
  "colname1": "string!",
  "colname3": "float!"
}
```

Or the same as list (order is important):

```
[
  "string!",
  "int",
  "float!"
]
```

You can add the types via the `--types` parameter:

```
$ go-mssql-load --user sa --pass Passw0rd loadcsv --sep "	" --types sql/pokemon_types.json pokemon.pokemon sql/pokemon.csv
```

### SQL execution

`go-mssql-load` uses the
[batch mechanism](https://github.com/microsoft/go-mssqldb/blob/main/batch/batch.go) of
the [go-mssqldb](https://github.com/microsoft/go-mssqldb) lib. This means that each file
read and split into statements separated by a `GO` keyword.

Example: `./sql/init.sql`.

```console
$ go-mssql-load --user sa --pass Passw0rd loadsql sql/init.sql
```

### SQL querying

Similar to SQL execution, query scripts are split by the keyword `GO`. This means you
can have multiple query statements per file. Each query results in a set of
[Newline Delimited JSON](http://ndjson.org/) records, separated by `---`.

Example: `./sql/select.sql`

```console
$ go-mssql-load --user sa --pass Passw0rd querysql sql/select.sql  2>/dev/null
{"name":"Wartortle"}
---
{"hp":4}
```
