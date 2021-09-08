import csv
import os

from configparser import ConfigParser
from flask import abort, Flask, request, Response
from io import StringIO
from sqlalchemy import create_engine


app = Flask(__name__)

DB = os.environ.get("SPROCKET_DB")
if not DB:
    raise NameError("'SPROCKET_DB' environment variable must be set")
if DB.endswith(".db"):
    abspath = os.path.abspath(DB)
    db_url = "sqlite:///" + abspath + "?check_same_thread=False"
    engine = create_engine(db_url)
    CONN = engine.connect()
elif DB.endswith(".ini"):
    config_parser = ConfigParser()
    config_parser.read(DB)
    if config_parser.has_section("postgresql"):
        params = {}
        for param in config_parser.items("postgresql"):
            params[param[0]] = param[1]
    else:
        raise ValueError(
            "Unable to create database connection; missing [postgresql] section from " + DB
        )
    pg_user = params.get("user")
    if not pg_user:
        raise ValueError(
            "Unable to create database connection: missing 'user' parameter from " + DB
        )
    pg_pw = params.get("password")
    if not pg_pw:
        raise ValueError(
            "Unable to create database connection: missing 'password' parameter from " + DB
        )
    pg_db = params.get("database")
    if not pg_db:
        raise ValueError(
            "Unable to create database connection: missing 'database' parameter from " + DB
        )
    pg_host = params.get("host", "127.0.0.1")
    pg_port = params.get("port", "5432")
    db_url = f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(db_url)
    CONN = engine.connect()
else:
    raise ValueError(
        "Either a database file or a config file must be specified with a .db or .ini extension"
    )


@app.route("/<table>", methods=["GET"])
def get_table(table):
    limit = request.args.get("limit", "100")
    try:
        limit = int(limit)
    except ValueError:
        return abort(422, "'limit' must be an integer")
    if table not in get_sql_tables():
        return abort(422, f"'{table}' is not a valid table in the database")
    fmt = request.args.get("format", "tsv")
    if fmt not in ["tsv", "csv"]:
        return abort(422, f"'format' must be 'tsv' or 'csv', not '{fmt}'")
    select = request.args.get("select")
    if select:
        select_cols = select.split(",")
        table_cols = get_sql_columns(table)
        invalid_cols = list(set(select_cols) - set(table_cols))
        if invalid_cols:
            return abort(
                422,
                f"The following column(s) do not exist in '{table}' table: "
                + ", ".join(invalid_cols),
            )
        select = ", ".join(select_cols)
    else:
        select = "*"
    # We can't use parameters for these values, but we've already validated that table exists
    # and that limit is an integer, so this query should be safe
    results = CONN.execute(f"SELECT {select} FROM {table} LIMIT {limit}")
    headers = results.keys()
    output = StringIO()
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    writer.writerow(list(headers))
    writer.writerows(list(results))
    return Response(output.getvalue(), mimetype="text/tab-separated-values")


def get_sql_columns(table):
    """Get a list of columns from a table."""
    # Check for required columns
    if str(CONN.engine.url).startswith("sqlite"):
        res = CONN.execute(f"PRAGMA table_info({table})")
    else:
        res = CONN.execute(
            f"""SELECT column_name AS name, data_type AS type FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME = '{table}';"""
        )
    return {x["name"]: x["type"] for x in res}


def get_sql_tables():
    """Get a list of tables from a database. Taken from ontodev-gizmos."""
    if str(CONN.engine.url).startswith("sqlite"):
        res = CONN.execute("SELECT name FROM sqlite_master WHERE type='table';")
    else:
        res = CONN.execute(
            "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = 'public'"
        )
    return [x["name"] for x in res]


def main():
    app.run()


if __name__ == "__main__":
    main()
