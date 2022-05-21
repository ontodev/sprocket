import os
import shutil

from argparse import ArgumentParser
from configparser import ConfigParser
from flask import abort, Flask, Blueprint, render_template, request
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from typing import Optional
from urllib.parse import urlparse
from wsgiref.handlers import CGIHandler
from .render import render_database_table, render_swagger_table
from .lib import get_sql_tables, get_swagger_tables, SprocketError

BLUEPRINT = Blueprint(
    "sprocket",
    __name__,
    template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "templates")),
)

CONN = None  # type: Optional[Connection]
DB = None  # type: Optional[str]
DEFAULT_LIMIT = 100
DEFAULT_TABLE = None  # type: Optional[str]

# TODO: select is not maintained when using a filter


@BLUEPRINT.route("/", methods=["GET"])
def show_tables():
    if DEFAULT_TABLE:
        try:
            return render_database_table(CONN, DEFAULT_TABLE, request.args)
        except SprocketError as e:
            abort(422, str(e))
    if CONN:
        tables = get_sql_tables(CONN)
    else:
        tables = get_swagger_tables(DB)
    return render_template("index.html", title="sprocket", tables=tables)


@BLUEPRINT.route("/<table>", methods=["GET"])
def get_table_by_name(table):
    if table == "favicon.ico":
        return render_template("test.html")
    try:
        if CONN:
            return render_database_table(CONN, table, request.args, default_limit=DEFAULT_LIMIT)
        else:
            return render_swagger_table(DB, table, request.args, default_limit=DEFAULT_LIMIT)
    except SprocketError as e:
        abort(422, str(e))


def prepare(db, table=None, limit=None):
    """Prepare the global vars for running sprocket:
    - CONN: database connection created from DB (None when DB is a Swagger endpoint)
    - DB: SQLite database file, Postgres config file, or Swagger endpoint URL
    - DEFAULT_LIMIT: max number of results to display on a page when limit is not in query params
    - DEFAULT_TABLE: table to redirect to from index page

    :param db: SQLite database file, Postgres config file, or Swagger endpoint URL
    :param table: table to set as DEFAULT_TABLE
    :param limit: int to set as DEFAULT_LIMIT
    """
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
            raise SprocketError(
                "Unable to create database connection; missing [postgresql] section from " + DB
            )
        pg_user = params.get("user")
        if not pg_user:
            raise SprocketError(
                "Unable to create database connection: missing 'user' parameter from " + DB
            )
        pg_pw = params.get("password")
        if not pg_pw:
            raise SprocketError(
                "Unable to create database connection: missing 'password' parameter from " + DB
            )
        pg_db = params.get("database")
        if not pg_db:
            raise SprocketError(
                "Unable to create database connection: missing 'database' parameter from " + DB
            )
        pg_host = params.get("host", "127.0.0.1")
        pg_port = params.get("port", "5432")
        db_url = f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(db_url)
        CONN = engine.connect()
    else:
        # Assume this is a Swagger endpoint, check that it is a well-formed URL
        # (if it isn't an endpoint, sprocket will fail when we try to query)
        res = urlparse(DB)
        if not all([res.scheme, res.netloc]):
            raise SprocketError("Unable to parse endpoint URL: " + DB)


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
    if args.cgi:
        CGIHandler().run(app)
    else:
        app.run()


if __name__ == "__main__":
    main()
