# sprocket

Sprocket presents a REST API and hypertext interface for database tables.

## Usage

To install `sprocket` and its requirements, simply run:
```bash
python3 -m pip install .
```

If you are using a Postgres database, you must also have the [`psycopg2`](https://pypi.org/project/psycopg2/) module installed.

To run `sprocket`, you must include the path to your database (for SQLite) or database configuration `.ini` file (for Postgres):
```bash
sprocket database.db
```

This will start the server on `localhost:5000`.

Alternatively, you can provide the URL to a [PostgREST OpenAPI](https://postgrest.org/en/v9.0/api.html#openapi-support) (aka Swagger) endpoint, such as https://www.cmi-pb.org/api/v2. Each request will be sent to the endpoint and the JSON results will be displayed as the same HTML table as providing a database.
```bash
sprocket https://www.cmi-pb.org/api/v2
```

The first time we send a request to the API for a given table, `sprocket` will store some details in a cache directory `.swagger`. This includes all column names in the table and total results. For large datasets, the first time you load the table may take a little bit longer. The cache is removed when `sprocket` exits, but if you wish to keep it to speed up the results for future runs, you can do so by including the `-s`/`--save-cache` flag. This should not be used if the data in the database is changing between runs.

### Usage in Python

You can also choose to run your own Flask app that uses `sprocket` as a [Blueprint](https://flask.palletsprojects.com/en/2.0.x/blueprints/). This is useful if you'd like to provide a URL prefix, as shown in the example below. Replace `PATH_TO_DATABASE` with your SQLite or PostgreSQL database, or a Swagger endpoint. You must call the `prepare` function to set some important global variables and create the database connection.

```python
from flask import Flask
from sprocket import BLUEPRINT, prepare

app = Flask(__name__)
app.register_blueprint(BLUEPRINT, url_prefix="/sprocket")
prepare(PATH_TO_DATABASE)

if __name__ == '__main__':
    app.run()
```

## Testing

To run a test version of `sprocket`, use the SQL file at `tests/resources/test.sql` to generate a new database:
```bash
sqlite3 test.db < tests/resources/test.sql
```

Then start the `sprocket` server with the default table set to `test1`:
```bash
sprocket test.db -t test
```

## Command Line Options

### Default table

When running `sprocket` with no additional arguments, the base path (`/`) will not resolve. To set this path to a default table, include the `-t`/`--table` option:
```bash
sprocket database.db -t tablename
```

### Limits

`sprocket` will show 100 results per page by default when you first view a table. This can always be changed using the HTML form or the `limit` query parameter, but if you wish to change the default you can do so with `-l`/`--limit`. For example, to always show 20 results when viewing a table:
```bash
sprocket database.db -l 20
```

### CGI script

You can also run `sprocket` as a CGI script using the `-c`/`--cgi` flag. For example, you can create a `sprocket.sh` script with the following content:
```bash
#!/usr/bin/env bash

sprocket database.db -t tablename -c
```

Your server may need more configuration to run this, see [Server Setup](https://flask.palletsprojects.com/en/2.0.x/deploying/cgi/#server-setup) in the Flask documentation.

## Paths

### /\<table\>

When provided with a table name (which must exist in the database), sending a GET request to this path will return the first 100 results from that table. By default, this is an HTML page, but you can choose to get a `tsv` or `csv` table using the `format` parameter below.

Optional query parameters:
* `format`: Export the results in given format, must be `html` (default), `tsv`, or `csv`
* `limit`: Return a different number of results, must be an integer
* `offset`: Return results starting after given integer (e.g., `offset=5` will return results starting with the 6th result)
* `order`: See [ORDER BY Clauses](#order-by-clauses)
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

#### ORDER BY Clauses

You can use the `order` query parameter to define one or more columns to sort on. By default, this is ascending. Multiple values should be comma-separated, no whitespace.
```
/<table>?order=subject
/<table>?order=subject,object
```

You can include `asc` or `desc` keywords to specify ascending or descending results:
```
/<table>?order=subject.desc
/<table>?order=subject.desc,object.desc
```

Finally, you can specify if you wish to display `nullsfirst` or `nullslast`. These should always be the last keyword.
```
/<table>?order=subject.desc.nullsfirst
/<table>?order=subject.nullsfirst
```
