import csv
import os

from configparser import ConfigParser
from flask import abort, Flask, request, Response
from grammar import PARSER, SprocketTransformer
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text


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
    if table not in get_sql_tables():
        return abort(422, f"'{table}' is not a valid table in the database")
    table_cols = get_sql_columns(table)

    limit = request.args.get("limit", "100")
    if limit.lower() == "none":
        limit = -1
    else:
        try:
            limit = int(limit)
        except ValueError:
            return abort(422, "'limit' must be an integer or 'none'")

    offset = request.args.get("offset", "0")
    try:
        offset = int(offset)
    except ValueError:
        return abort(422, "'offset' must be an integer")

    limit = limit + offset

    fmt = request.args.get("format", "tsv")
    if fmt not in ["tsv", "csv"]:
        return abort(422, f"'format' must be 'tsv' or 'csv', not '{fmt}'")

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
        select = ", ".join(select_cols)
    else:
        select = "*"

    where_statements = []
    for tc in table_cols:
        where = request.args.get(tc)
        if not where:
            continue
        where_statements.append(get_where(where, tc))

    # Build & execute the query
    results = exec_query(table, select, where_statements=where_statements, limit=limit)
    headers = results.keys()
    output = StringIO()
    sep = "\t"
    mt = "text/tab-separated-values"
    if fmt == "csv":
        sep = ","
        mt = "text/comma-separated-values"
    writer = csv.writer(output, delimiter=sep, lineterminator="\n")
    writer.writerow(list(headers))
    writer.writerows(list(results)[offset:])
    return Response(output.getvalue(), mimetype=mt)


def exec_query(table, select, where_statements=None, limit=100):
    """Create a query from, minimally, a table name and a select statement."""
    query = f"SELECT {select} FROM {table}"
    const_dict = {}
    # Add keys for any where statements using user input values
    if where_statements:
        n = 0
        expanded_statements = []
        for ws, constraint in where_statements:
            if not constraint:
                expanded_statements.append(ws)
                continue
            k = f"const{n}"
            ws += f" :{k}"
            const_dict[k] = constraint
            expanded_statements.append(ws)
            n += 1
        query += " WHERE " + " AND ".join(expanded_statements)
    if limit > 0:
        query += f" LIMIT {limit}"
    query = sql_text(query)
    for k, v in const_dict.items():
        if isinstance(v, list):
            query = query.bindparams(bindparam(k, expanding=True))
        else:
            query = query.bindparams(bindparam(k))
    return CONN.execute(query, const_dict)


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


def get_where(where, column):
    """Create a where clause by parsing the horizontal filtering condition."""
    # Parse using Lark grammar
    parsed = PARSER.parse(where)
    res = SprocketTransformer().transform(parsed)
    if len(res) == 3:
        # NOT operator included
        operator = res[1]
        constraint = res[2]
        statement = "NOT "
    else:
        operator = res[0]
        constraint = res[1]
        statement = ""

    # Some basic validation
    if operator != "in" and isinstance(constraint, list):
        return abort(422, f"The constraint for '{operator}' must be a single value, not a list")
    elif operator == "in" and not isinstance(constraint, list):
        return abort(422, "The constraint for 'in' must be a list")

    # Set the SQL operator
    if operator == "eq":
        query_op = "="
    elif operator == "gt":
        query_op = ">"
    elif operator == "gte":
        query_op = ">="
    elif operator == "lt":
        query_op = "<"
    elif operator == "lte":
        query_op = "<="
    elif operator == "neq":
        query_op = "!="
    elif operator == "is":
        if constraint.lower() == "true":
            query_op = "IS TRUE"
            constraint = None
        elif constraint.lower() == "false":
            query_op = "IS FALSE"
            constraint = None
        elif constraint.lower() == "null":
            query_op = "IS NULL"
            constraint = None
        else:
            query_op = "IS"
    else:
        query_op = operator.upper()
    return statement + f"{column} {query_op}", constraint


def main():
    app.run()


if __name__ == "__main__":
    main()
