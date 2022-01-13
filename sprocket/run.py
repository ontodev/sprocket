import csv
import os
import requests
import shutil

from argparse import ArgumentParser
from configparser import ConfigParser
from flask import abort, Flask, Blueprint, render_template, request, Response
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from typing import Optional
from wsgiref.handlers import CGIHandler
from .lib import (
    exec_query,
    get_sql_columns,
    get_sql_tables,
    get_swagger_details,
    get_swagger_tables,
    parse_order_by,
    parse_where,
    render_html_table,
    render_tsv_table,
)

BLUEPRINT = Blueprint(
    "sprocket",
    __name__,
    template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "templates")),
)

CONN = None  # type: Optional[Connection]
DB = None  # type: Optional[str]
DEFAULT_LIMIT = 100
DEFAULT_TABLE = None
SWAGGER_CACHE = ".swagger"

# TODO: select is not maintained when using a filter


@BLUEPRINT.route("/", methods=["GET"])
def show_tables():
    if DEFAULT_TABLE:
        return get_table_from_database(DEFAULT_TABLE)
    if CONN:
        tables = get_sql_tables(CONN)
    else:
        tables = get_swagger_tables(DB)
    return render_template("index.html", title="sprocket", tables=tables)


@BLUEPRINT.route("/<table>", methods=["GET"])
def get_table_by_name(table):
    if table == "favicon.ico":
        return render_template("test.html")
    if CONN:
        return get_table_from_database(table)
    else:
        return get_table_from_swagger(table)


def get_table_from_database(table, hide_meta=True):
    """Get the SQL table for the Flask app."""
    if table not in get_sql_tables(CONN):
        return abort(422, f"'{table}' is not a valid table in the database")
    table_cols = get_sql_columns(CONN, table)

    limit = request.args.get("limit", DEFAULT_LIMIT)
    try:
        limit = int(limit)
    except ValueError:
        return abort(422, "'limit' must be an integer")

    offset = request.args.get("offset", "0")
    try:
        offset = int(offset)
    except ValueError:
        return abort(422, "'offset' must be an integer")

    limit = limit + offset

    fmt = request.args.get("format", "html")
    if fmt not in ["tsv", "csv", "html"]:
        return abort(422, f"'format' must be 'tsv', 'csv', or 'html', not '{fmt}'")

    select = request.args.get("select")
    if select:
        select_cols = select.split(",")
        invalid_cols = list(set(select_cols) - set(table_cols))
        if invalid_cols:
            return abort(
                422,
                f"The following column(s) do not exist in '{table}' table: "
                + ", ".join(invalid_cols),
            )
        if hide_meta:
            # Add any necessary meta cols, since they don't appear in select filters
            select_cols.extend([f"{x}_meta" for x in select_cols if f"{x}_meta" in table_cols])
    else:
        select_cols = ["*"]

    where_statements = []
    for tc in table_cols:
        where = request.args.get(tc)
        if not where:
            continue
        try:
            stmt = parse_where(where, tc)
        except ValueError as e:
            return abort(422, str(e))
        where_statements.append(stmt)

    order_by = []
    order = request.args.get("order")
    if order:
        try:
            order_by = []
            for ob in parse_order_by(order):
                s = [ob["key"]]
                if ob["order"]:
                    s.append(ob["order"].upper())
                if ob["nulls"]:
                    s.append("NULLS " + ob["nulls"].upper())
                order_by.append(" ".join(s))
        except ValueError as e:
            return abort(422, str(e))

    violations = request.args.get("violations")
    if violations:
        violations = violations.split(",")
        for v in violations:
            if v not in ["debug", "info", "warn", "error"]:
                return abort(
                    422,
                    f"'violations' contains invalid level '{v}' - must be one of: debug, info, warn, error",
                )

    # Build & execute the query
    results = exec_query(
        CONN,
        table,
        columns=table_cols,
        select=select_cols,
        where_statements=where_statements,
        order_by=order_by,
        violations=violations,
    )

    # Return results based on format
    if fmt == "html":
        return render_html_table(
            results, table, table_cols, request.args, default_limit=DEFAULT_LIMIT
        )
    headers = results[0].keys()
    output = StringIO()
    sep = "\t"
    mt = "text/tab-separated-values"
    if fmt == "csv":
        sep = ","
        mt = "text/comma-separated-values"
    writer = csv.writer(output, delimiter=sep, lineterminator="\n")
    writer.writerow(list(headers))
    writer.writerows(list(results)[offset : limit + offset])
    return Response(output.getvalue(), mimetype=mt)


def get_table_from_swagger(table, limit=100):
    if not os.path.exists(SWAGGER_CACHE):
        os.mkdir(SWAGGER_CACHE)

    # Create a URL to get JSON from
    url = DB + "/" + table
    swagger_request_args = []
    get_all_columns = False

    # Parse args and create request
    fmt = None
    has_limit = False
    for arg, value in request.args.items():
        if arg == "select":
            get_all_columns = True
        if arg == "limit":
            has_limit = True
            limit = int(value)
        if arg == "violations":
            continue
        if arg == "format":
            fmt = value
            continue
        swagger_request_args.append(f"{arg}={value}")
    if not has_limit:
        # We always want to have the limit
        swagger_request_args.append(f"limit={limit}")

    # Send request and get data + total rows
    if swagger_request_args:
        url += "?" + "&".join(swagger_request_args)
    r = requests.get(url, verify=False)
    data = r.json()

    # Error from API
    if type(data) == dict:
        if data.get("message") and data.get("details"):
            msg = data["message"]
            details = data["details"]
        else:
            msg = "Unable to complete query"
            details = "Please revise query and try again."
        return render_template(
            "test.html",
            title=table,
            default=f"<div class='container'><h2>{msg}</h2><p>{details}</p></div>",
        )

    if fmt:
        # Save to TSV or CSV, just returning that response
        mt = "tab-separated-values"
        if fmt == "csv":
            mt = "comma-separated-values"
        output = render_tsv_table(data, fmt=fmt)
        return Response(output, mimetype=mt)

    columns, total = get_swagger_details(
        DB, table, data, get_all_columns=get_all_columns, swagger_cache=SWAGGER_CACHE
    )
    return render_html_table(data, table, columns, request.args, total=total, default_limit=DEFAULT_LIMIT)


def prepare(db, table=None, limit=None):
    global CONN, DB, DEFAULT_LIMIT, DEFAULT_TABLE
    if limit:
        DEFAULT_LIMIT = limit
    if table:
        DEFAULT_TABLE = table
    DB = db
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
    # TODO: error handling for invalid swagger URL
    # else:
    # raise ValueError(
    # "Either a database file or a config file must be specified with a .db or .ini extension"
    # )


def main():
    parser = ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("-t", "--table", help="Default table to show")
    parser.add_argument("-l", "--limit", help="Default limit for results (default: 100)", type=int)
    parser.add_argument("-c", "--cgi", help="Run as CGI script", action="store_true")
    parser.add_argument("-s", "--save-cache", help="Save Swagger cache", action="store_true")
    args = parser.parse_args()

    # Set up some globals and the database connection
    prepare(args.db, table=args.table, limit=args.limit)

    # Register blueprint and run app
    app = Flask(__name__)
    app.register_blueprint(BLUEPRINT)
    app.url_map.strict_slashes = False
    try:
        if args.cgi:
            CGIHandler().run(app)
        else:
            app.run()
    finally:
        # Remove our tracked data from Swagger
        if not args.save_cache and os.path.exists(SWAGGER_CACHE):
            shutil.rmtree(SWAGGER_CACHE)


if __name__ == "__main__":
    main()
