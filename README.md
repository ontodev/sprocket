# sprocket

Sprocket presents a REST API and hypertext interface for database tables.

## Usage

To install `sprocket` and its requirements, simply run:
```bash
python3 -m pip install .
```

If you are using a Postgres database, you must also have the [`psycopg2`](https://pypi.org/project/psycopg2/) module installed.

To run `sprocket`, you must set the path to your database (for SQLite) or database configuration `.ini` file (for Postgres), then run the CLI:
```bash
export SPROCKET_DB=database.db
sprocket
```

This will start the server on `localhost:5000`.

## Paths

### /\<table\>

When provided with a table name (which must exist in the database), sending a GET request to this path will return the first 100 results from that table in TSV format.

Optional query parameters:
* `format`: Export the results in given format, must be either `tsv` (default) or `csv`
* `limit`: Return a different number of results, must be an integer
* `offset`: Return results starting after given integer (e.g., `offset=5` will return results starting with the 6th result)
* `select`: A comma-separated list of columns to include in results (no spaces)

#### WHERE Clauses

You can also include the names of columns as optional query parameters where the value is one of the [hortizontal filtering](https://postgrest.org/en/v8.0/api.html#horizontal-filtering-rows) conditions. The general pattern is `<table>?<column>=<operator>.<constraint>`.

The following operators are currently supported:

| Operator | Meaning                         |
| -------- | ------------------------------- |
| eq       | equals                          |
| gt       | greater than                    |
| gte      | greater than or equal           |
| lt       | less than                       |
| lte      | less than or equal              |
| neq      | not equal                       |
| like     | SQL LIKE (use * in place of %)  |
| ilike    | case insensitive LIKE           |
| is       | exact equal (true, false, null) |
| in       | one of list values              |

For example, to restrict the `subject` column to values equal to the string "foo":
```
/<table>?subject=eq.foo
```

If the constraint of the condition contains a comma or parentheses, it must be enclosed in double quotes. Strings with whitespace do not need to be enclosed, but you can if you prefer.
```
/<table>?subject=eq."foo (bar)"
/<table>?subject=eq."foo, bar, baz"
/<table>?subject=eq.foo bar
```

The `in` condition accepts a list as a constraint, which is a comma-separated list (NO whitespace, unless the constraint contains whitespace) of values enclosed in parentheses:
```
/<table>?subject=in.(foo,bar,baz)
```

You can negate an operator by including the `not` operator:
```
/<table>?subject=not.in.(foo,bar,baz)
```
