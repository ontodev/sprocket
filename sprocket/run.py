import csv
import json
import os
import ssl

from argparse import ArgumentParser
from copy import deepcopy
from configparser import ConfigParser
from flask import abort, Flask, render_template, request, Response
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Optional
from urllib.request import urlopen
from wsgiref.handlers import CGIHandler
from .grammar import PARSER, SprocketTransformer

ssl._create_default_https_context = ssl._create_unverified_context

app = Flask(
    __name__, template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "resources"))
)
app.url_map.strict_slashes = False

CONN = None  # type: Optional[Connection]
DB = None  # type: Optional[str]
FILTER_OPTS = {
    "eq": {"label": "equals"},
    "gt": {"label": "greater than"},
    "gte": {"label": "greater than or equals"},
    "lt": {"label": "less than"},
    "lte": {"label": "less than or equals"},
    "neq": {"label": "not equals"},
    "like": {"label": "like"},
    "ilike": {"label": "like (insensitive)"},
    "is": {"label": "is"},
    "not.is": {"label": "is not"},
    "in": {"label": "in"},
    "not.in": {"label": "not in"},
}


@app.route("/<table>", methods=["GET"])
def get_table_by_name(table):
    return get_table(table)


def add_table(data, table):
    """Add a table to the database from Swagger endpoint data."""
    # Create the table
    lines = []
    col_to_type = get_sql_types(data)
    for c, t in col_to_type.items():
        lines.append(f"'{c}' {t}")
    CONN.execute(f"CREATE TABLE {table} (\n" + ",\n".join(lines) + "\n);")
    # Add data to table
    for row in data:
        vals = []
        i = 0
        d = {}
        for value in row.values():
            if value is None:
                vals.append("NULL")
            else:
                i += 1
                vals.append(f":val{i}")
                d[f"val{i}"] = value
        query = sql_text(f"INSERT INTO {table} VALUES (" + ", ".join(vals) + ");")
        CONN.execute(query, d)


def exec_query(table, select, where_statements=None, order_by=None, violations=None):
    """Create a query from, minimally, a table name and a select statement."""
    query = f"SELECT {', '.join(select)} FROM {table}"
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
    if violations:
        # Make sure to start this part of the query correctly
        if not where_statements:
            query += " WHERE "
        else:
            query += " AND "
        # For each *_meta column, add LIKE filters for the violation levels
        meta_cols = [x for x in select if x.endswith("_meta")]
        meta_filters = []
        for m in meta_cols:
            likes = []
            for v in violations:
                likes.append(f'{m} LIKE \'%"level": "{v}"%\'')
            meta_filters.append("(" + " OR ".join(likes) + ")")
        query += " AND ".join(meta_filters)
    if order_by:
        query += " ORDER BY " + ", ".join(order_by)
    query = sql_text(query)
    for k, v in const_dict.items():
        if isinstance(v, list):
            query = query.bindparams(bindparam(k, expanding=True))
        else:
            query = query.bindparams(bindparam(k))
    return CONN.execute(query, const_dict)


def get_order_by(order):
    """Create a list of ORDER BY statements from the order query param."""
    order_by = []
    for itm in order.split(","):
        attrs = itm.split(".")
        if len(attrs) == 1:
            order_by.append(attrs[0])
        elif len(attrs) == 2:
            if attrs[1] == "nullsfirst":
                order_by.append(f"{attrs[0]} NULLS FIRST")
            elif attrs[1] == "nullslast":
                order_by.append(f"{attrs[0]} NULLS LAST")
            elif attrs[1] == "asc":
                order_by.append(attrs[0])
            elif attrs[1] == "desc":
                order_by.append(f"{attrs[0]} DESC")
            else:
                return abort(422, "Unknown order qualifier: " + attrs[1])
        elif len(attrs) == 3:
            o = ""
            if attrs[1] == "desc":
                o = " DESC "
            elif attrs[1] != "asc":
                return abort(
                    422, "Second 'order' modifier must be either 'asc' or 'desc', not " + attrs[1],
                )
            if attrs[2] == "nullsfirst":
                n = "FIRST"
            elif attrs[2] == "nullslast":
                n = "LAST"
            else:
                return abort(
                    422,
                    "Third 'order' modifier must be either 'nullsfirst' or 'nullslast', not "
                    + attrs[2],
                )
            order_by.append(f"{attrs[0]}{o} NULLS {n}")
    return order_by


def get_remote_json(url):
    with urlopen(url) as response:
        data_str = response.read()
        encoding = response.headers.get_content_charset("utf-8")
        decoded_str = data_str.decode(encoding)
    return json.loads(decoded_str)


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


def get_sql_types(data, col_to_type=None):
    """Create a dictionary of column name to SQL type for all keys."""
    if not col_to_type:
        col_to_type = {}
    cols = data[0].keys()
    for itm in data:
        for h, v in itm.items():
            if v:
                t = type(v)
                if t == str:
                    col_to_type[h] = "TEXT"
                elif t == int:
                    col_to_type[h] = "INTEGER"
                elif t == float:
                    col_to_type[h] = "FLOAT"
                else:
                    col_to_type[h] = "BLOB"
        if set(cols) == set(col_to_type.keys()):
            return col_to_type
    missing = set(cols) - set(col_to_type.keys())
    for m in missing:
        col_to_type[m] = "NULL"
    return col_to_type


def get_table(table, hide_meta=True):
    """Get the SQL table for the Flask app."""
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
        where_statements.append(get_where(where, tc))

    order_by = []
    order = request.args.get("order")
    if order:
        order_by = get_order_by(order)

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
        table,
        select_cols,
        where_statements=where_statements,
        order_by=order_by,
        violations=violations,
    )

    # Return results based on format
    if fmt == "html":
        return render_html(results, table, table_cols, request.args)
    headers = results.keys()
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


def render_html(results, table, columns, request_args, hide_meta=True):
    """Render the results as an HTML table."""
    header_names = list(results.keys())
    if hide_meta:
        # exclude *_meta columns from display and use the values to render cell styles
        meta_names = [x for x in header_names if x.endswith("_meta")]
        header_names = [x for x in header_names if x not in meta_names]
        # also update columns for selections
        columns = [x for x in columns if not x.endswith("_meta")]
        # iter through results and update
        res_updated = []
        for res in results:
            res = {k: {"value": v, "style": None, "message": None} for k, v in dict(res).items()}
            for m in meta_names:
                meta = res[m]["value"]
                del res[m]
                if not meta:
                    continue
                data = json.loads(meta[5:-1])
                value_col = m[:-5]
                # Set the value to what is given in the JSON (as "value")
                res[value_col]["value"] = data["value"]
                if "nulltype" in data:
                    # Set null style and go to next
                    res[value_col]["style"] = "null"
                    continue
                # Use a number for violation level to make sure the "worst" violation is displayed
                violation_level = -1
                messages = []
                if "messages" in data:
                    for msg in data["messages"]:
                        lvl = msg["level"]
                        messages.append(lvl.upper() + ": " + msg["message"])
                        if lvl == "error":
                            violation_level = 3
                        elif lvl == "warn" and violation_level < 3:
                            violation_level = 2
                        elif lvl == "info" and violation_level < 2:
                            violation_level = 1
                        elif lvl == "debug" and violation_level < 1:
                            violation_level = 0
                # Set cell style based on violation level
                if violation_level == 0:
                    res[value_col]["style"] = "debug"
                elif violation_level == 1:
                    res[value_col]["style"] = "info"
                elif violation_level == 2:
                    res[value_col]["style"] = "warn"
                elif violation_level == 3:
                    res[value_col]["style"] = "error"
                # Join multiple messages with line breaks
                res[value_col]["message"] = "\n".join(messages)
            res_updated.append(list(res.values()))
        results = res_updated
    else:
        # No styles or tooltip messages
        results = [{"value": x, "style": None, "message": None} for x in list(results)]

    offset = int(request_args.get("offset", "0"))
    limit = int(request_args.get("limit", "100"))
    total = len(results)
    results = list(results)[offset : limit + offset]

    # Set the options for the "results per page" drop down
    options = []
    limit_vals = [10, 50, 100, 500]
    if limit not in limit_vals:
        limit_vals.append(limit)
    limit_vals = sorted(limit_vals)
    for lv in limit_vals:
        # Make sure the 'selected' value is our current limit
        if lv == limit:
            options.append(f'<option value="{lv}" selected>{lv}</option>')
        else:
            options.append(f'<option value="{lv}">{lv}</option>')

    # Set the options for filtering
    headers = {}
    for h in header_names:
        fltr = request_args.get(h)
        if not fltr:
            headers[h] = {"options": FILTER_OPTS, "has_selected": False}
            continue
        cur_options = deepcopy(FILTER_OPTS)
        opt = fltr.rsplit(".", 1)[0]
        val = fltr.rsplit(".", 1)[1]
        cur_options[opt]["selected"] = True
        headers[h] = {"options": cur_options, "const": val}

    # Set the options for violation filtering
    violations = request_args.get("violations", "").split(",")

    # Get URLs for "previous" and "next" links
    url = "./" + table
    prev_url = None
    next_url = None
    if offset > 0:
        # Only include "previous" link if we aren't at the beginning
        prev_args = request_args.copy()
        prev_offset = offset - limit
        if prev_offset < 0:
            prev_offset = 0
        prev_args["offset"] = prev_offset
        prev_query = [f"{k}={v}" for k, v in prev_args.items()]
        prev_url = url + "?" + "&".join(prev_query)
    if limit + offset < total:
        # Only include "next" link if we aren't at the end
        next_args = request_args.copy()
        next_args["offset"] = limit + offset
        next_query = [f"{k}={v}" for k, v in next_args.items()]
        next_url = url + "?" + "&".join(next_query)

    # Current URL is used for download links
    this_url = url + "?" + "&".join([f"{k}={v}" for k, v in request_args.items()])

    return render_template(
        "template.html",
        select=columns,
        options=options,
        headers=headers,
        violations=violations,
        rows=results,
        offset=offset,
        limit=limit,
        total=total,
        this_url=this_url,
        prev_url=prev_url,
        next_url=next_url,
    )


def main():
    global CONN, DB
    parser = ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("-t", "--table", help="Default table to show")
    parser.add_argument("-c", "--cgi", help="Run as CGI script", action="store_true")
    parser.add_argument(
        "-s", "--save-database", help="Save a database from Swagger endpoint to given path"
    )
    args = parser.parse_args()

    DB = args.db
    delete_on_exit = None
    tables = []
    if not DB:
        raise NameError("'SPROCKET_DB' environment variable must be set")
    if DB.endswith(".db"):
        abspath = os.path.abspath(DB)
        db_url = "sqlite:///" + abspath + "?check_same_thread=False"
        engine = create_engine(db_url)
        CONN = engine.connect()
        tables = get_sql_tables()
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
        tables = get_sql_tables()
    else:
        # try as API endpoint
        try:
            # If we cannot open the DB param, it's not a URL
            data = get_remote_json(DB)
            # Validate that this is JSON for a Swagger endpoint
            if "swagger" not in data:
                raise ValueError
        except ValueError:
            raise ValueError(
                "A database file, a config file, or a Swagger endpoint URL must be specified"
            )
        # Read the endpoint to get the table names
        tables = []
        for path in data["paths"].keys():
            if path != "/":
                tables.append(path[1:])

        if args.save_database:
            db = args.save_database
            if os.path.exists(db):
                raise ValueError("A database already exists at " + db)
        else:
            # Use temp DB and delete when finished
            db = ".temp.db"
            delete_on_exit = ".temp.db"
        try:
            engine = create_engine(f"sqlite:///{db}?check_same_thread=False")
            CONN = engine.connect()
            for t in tables:
                data = get_remote_json(DB + "/" + t)
                add_table(data, t)
        except Exception as e:
            if delete_on_exit and os.path.exists(delete_on_exit):
                os.remove(delete_on_exit)
            raise e

    try:
        if args.table:
            # Maybe set the base route to provided default table
            @app.route("/", methods=["GET"])
            def get_default_table():
                return get_table(args.table)

        else:
            # Otherwise show a list of available tables
            @app.route("/", methods=["GET"])
            def show_all_tables():
                return render_template("index.html", tables=tables)

        if args.cgi:
            CGIHandler().run(app)
        else:
            app.run()
    finally:
        if delete_on_exit and os.path.exists(delete_on_exit):
            os.remove(delete_on_exit)


if __name__ == "__main__":
    main()
