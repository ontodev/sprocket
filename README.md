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
* `select`: A comma-separated list of columns to include in results (no spaces)
