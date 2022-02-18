import os
import shutil

from argparse import ArgumentParser
from configparser import ConfigParser
from flask import Flask, Blueprint, render_template
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from typing import Optional
from wsgiref.handlers import CGIHandler
from .render import render_database_table, render_swagger_table
from .lib import get_sql_tables, get_swagger_tables

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
        return render_database_table(CONN, DEFAULT_TABLE)
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
        return render_database_table(CONN, table, default_limit=DEFAULT_LIMIT)
    else:
        return render_swagger_table(DB, table, default_limit=DEFAULT_LIMIT, swagger_cache=SWAGGER_CACHE)


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
