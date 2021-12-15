import csv
import json
import os

import requests
import shutil

from argparse import ArgumentParser
from collections import defaultdict
from copy import deepcopy
from configparser import ConfigParser
from flask import abort, Blueprint, Flask, render_template, request, Response
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Optional
from wsgiref.handlers import CGIHandler
from .grammar import PARSER, SprocketTransformer


sprocket = Blueprint(
    "sprocket",
    __name__,
    template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "resources")),
)
# app = Flask(
#    __name__, template_folder=os.path.abspath(os.path.join(os.path.dirname(__file__), "resources"))
# )
# app.url_map.strict_slashes = False

CONN = None  # type: Optional[Connection]
DB = None  # type: Optional[str]
DEFAULT_LIMIT = 100
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
SWAGGER_CACHE = ".swagger"


@sprocket.route("/<table>", methods=["GET"])
def get_table_by_name(table):
    if table == "favicon.ico":
        return render_template("base.html")
    if CONN:
        return get_table_from_database(table)
    else:
        return get_table_from_swagger(table)


def exec_query(table, columns, select, where_statements=None, order_by=None, violations=None):
    """Create a query from, minimally, a table name, a list of all columns,
    and a subset of columns (or *)."""
    query = f"SELECT {', '.join(select)} FROM '{table}'"
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
        meta_cols = [x for x in columns if x.endswith("_meta")]
        meta_filters = []
        for m in meta_cols:
            likes = []
            for v in violations:
                likes.append(f'{m} LIKE \'%"level": "{v}"%\'')
            meta_filters.append("(" + " OR ".join(likes) + ")")
        query += " OR ".join(meta_filters)
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


def get_sql_columns(table):
    """Get a list of columns from a table."""
    # Check for required columns
    if str(CONN.engine.url).startswith("sqlite"):
        res = CONN.execute(f"PRAGMA table_info('{table}')")
    else:
        res = CONN.execute(
            f"""SELECT column_name AS name, data_type AS type FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME = '{table}';"""
        )
    return {x["name"]: x["type"] for x in res}


def get_sql_tables():
    """Get a list of tables from a database. Taken from ontodev-gizmos."""
    if str(CONN.engine.url).startswith("sqlite"):
        res = CONN.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_conflict';"
        )
    else:
        res = CONN.execute(
            """SELECT table_name AS name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name NOT LIKE '%_conflict';"""
        )
    return [x["name"] for x in res]


def get_swagger_details(table, data, get_all_columns=False):
    """Get a list of columns for a table from Swagger,
    checking first if we've cached the columns."""
    # Check for columns in the cache file
    table_columns = defaultdict(list)
    columns_file = os.path.join(SWAGGER_CACHE, "columns.tsv")
    if os.path.exists(columns_file):
        with open(columns_file, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if row[0] not in table_columns:
                    table_columns[row[0]] = list()
                table_columns[row[0]].append(row[1])
    columns = table_columns.get(table)

    total = None
    totals_file = os.path.join(SWAGGER_CACHE, "totals.tsv")
    totals_rows = []
    if os.path.exists(totals_file):
        with open(totals_file, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                totals_rows.append(row)
                if row[0] == table:
                    total = int(row[1])

    if not columns or total is None:
        if get_all_columns or total is None:
            # We need to send another request to get all columns if a select statement is used
            r = requests.get(
                f"{DB}/{table}?limit=1", headers={"Prefer": "count=estimated"}, verify=False
            )
            data2 = r.json()[0]
            columns = list(data2.keys())
            total = int(r.headers["Content-Range"].split("/")[1])
        else:
            # We already have the total and we have all the columns in the data - no need to hit API
            columns = list(data[0].keys())
        # Save updated cache file so we don't have to request again
        table_columns[table] = columns
        columns_rows = []
        for table, columns in table_columns.items():
            for col in columns:
                columns_rows.append([table, col])
        with open(columns_file, "w") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            writer.writerows(columns_rows)
        with open(totals_file, "w") as f:
            writer = csv.writer(f, delimiter="\t", lineterminator="\n")
            writer.writerows(totals_rows)

    return columns, total


def get_swagger_tables():
    # TODO: error handling
    r = requests.get(DB, verify=False)
    data = r.json()
    return [path[1:] for path, details in data["paths"].items() if path != "/" and "get" in details]


def get_table_from_database(table, hide_meta=True):
    """Get the SQL table for the Flask app."""
    if table not in get_sql_tables():
        return abort(422, f"'{table}' is not a valid table in the database")
    table_cols = get_sql_columns(table)

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
        table_cols,
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


def get_swagger_total(table):
    fname = os.path.join(SWAGGER_CACHE, "totals.tsv")
    total = None
    if os.path.exists(fname):
        with open(fname, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if row[0] == table:
                    total = int(row[1])
    if not total:
        pass


def get_table_from_swagger(table):
    if not os.path.exists(SWAGGER_CACHE):
        os.mkdir(SWAGGER_CACHE)

    # Create a URL to get JSON from
    url = DB + "/" + table
    request_args = []
    get_all_columns = False

    # Parse args and create request
    violations = []
    fmt = None
    limit = DEFAULT_LIMIT
    offset = 0
    has_limit = False
    for arg, value in request.args.items():
        if arg == "select":
            get_all_columns = True
        if arg == "limit":
            has_limit = True
            limit = int(value)
        if arg == "offset":
            offset = int(value)
        if arg == "violations":
            violations = value.split(",")
            continue
        if arg == "format":
            fmt = value
            continue
        request_args.append(f"{arg}={value}")
    if not has_limit:
        # We always want to have the limit
        request_args.append(f"limit={limit}")

    # Send request and get data + total rows
    if request_args:
        url += "?" + "&".join(request_args)
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
            "base.html",
            title=table,
            default=f"<div class='container'><h2>{msg}</h2><p>{details}</p></div>",
        )

    if fmt:
        # Save to TSV or CSV, just returning that response
        headers = data[0].keys()
        output = StringIO()
        sep = "\t"
        mt = "text/tab-separated-values"
        if fmt == "csv":
            sep = ","
            mt = "text/comma-separated-values"
        writer = csv.DictWriter(
            output, delimiter=sep, fieldnames=list(headers), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(list(data))
        return Response(output.getvalue(), mimetype=mt)

    # Set the options for filtering
    header_names = data[0].keys()
    headers = {}
    for h in header_names:
        fltr = request.args.get(h)
        if not fltr:
            headers[h] = {"options": FILTER_OPTS, "has_selected": False}
            continue
        cur_options = deepcopy(FILTER_OPTS)
        opt = fltr.rsplit(".", 1)[0]
        val = fltr.rsplit(".", 1)[1]
        cur_options[opt]["selected"] = True
        headers[h] = {"options": cur_options, "const": val}

    # Get all columns for select
    columns, total = get_swagger_details(table, data, get_all_columns)

    data = [list(x.values()) for x in data]
    results = []
    for d in data:
        results.append([{"value": x, "style": None, "message": None} for x in d])

    # Set the options for the "results per page" drop down
    options = []
    limit_vals = [1, 10, 50, 100, 500, total]
    if limit not in limit_vals:
        limit_vals.append(limit)
    limit_vals = sorted(limit_vals)
    for lv in limit_vals:
        # Make sure the 'selected' value is our current limit
        if lv == limit:
            options.append(f'<option value="{lv}" selected>{lv}</option>')
        else:
            options.append(f'<option value="{lv}">{lv}</option>')

    prev_url, next_url, this_url = get_urls(table, request.args, total, offset=offset, limit=limit)

    render_args = {
        "title": table,
        "select": columns,
        "options": options,
        "violations": violations,
        "offset": offset,
        "headers": headers,
        "total": total,
        "limit": limit,
        "this_url": this_url,
        "prev_url": prev_url,
        "next_url": next_url,
    }
    if limit == 1 or total == 1:
        row = {}
        i = 0
        for h in headers:
            row[h] = results[0][i]
            i += 1
        render_args["row"] = row
        template = "vertical.html"
    else:
        render_args["rows"] = results
        template = "horizontal.html"
    return render_template(template, **render_args)


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
                # Get the metadata as JSON
                meta = res[m]["value"]
                del res[m]
                if not meta:
                    continue
                data = json.loads(meta[5:-1])

                if data.get("valid") and not data.get("nulltype"):
                    # Cell is not a null & is valid, nothing to style or change
                    continue

                # This is the name of the column we are editing
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
    limit = int(request_args.get("limit", DEFAULT_LIMIT))
    total = len(results)
    results = list(results)[offset : limit + offset]

    # Set the options for the "results per page" drop down
    options = []
    limit_vals = {1, 10, 50, 100, 500, total}
    if limit not in limit_vals:
        limit_vals.add(limit)
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

    prev_url, next_url, this_url = get_urls(table, request_args, total, offset=offset, limit=limit)

    render_args = {
        "title": table,
        "select": columns,
        "options": options,
        "violations": violations,
        "offset": offset,
        "headers": headers,
        "total": total,
        "limit": limit,
        "this_url": this_url,
        "prev_url": prev_url,
        "next_url": next_url,
    }
    if limit == 1 or total == 1:
        row = {}
        i = 0
        for h in headers:
            row[h] = results[0][i]
            i += 1
        render_args["row"] = row
        template = "vertical.html"
    else:
        render_args["rows"] = results
        render_args["headers"] = headers
        template = "horizontal.html"
    return render_template(template, **render_args)


def get_urls(table, request_args, total_results, offset=0, limit=DEFAULT_LIMIT):
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
    if limit + offset < total_results:
        # Only include "next" link if we aren't at the end
        next_args = request_args.copy()
        next_args["offset"] = limit + offset
        next_query = [f"{k}={v}" for k, v in next_args.items()]
        next_url = url + "?" + "&".join(next_query)

    # Current URL is used for download links
    this_url = url + "?" + "&".join([f"{k}={v}" for k, v in request_args.items()])

    return prev_url, next_url, this_url


def prepare(db, table=None, limit=None):
    global CONN, DB, DEFAULT_LIMIT
    if limit:
        DEFAULT_LIMIT = limit
    DB = db
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
    # else:
    # raise ValueError(
    # "Either a database file or a config file must be specified with a .db or .ini extension"
    # )

    # Maybe set the base route to provided default table
    if table:

        @sprocket.route("/", methods=["GET"])
        def get_default_table():
            return get_table_from_database(table)

    else:

        @sprocket.route("/", methods=["GET"])
        def show_tables():
            if CONN:
                tables = get_sql_tables()
            else:
                tables = get_swagger_tables()
            return render_template("index.html", title="sprocket", tables=tables)


def main():
    global CONN, DB, DEFAULT_LIMIT
    parser = ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("-t", "--table", help="Default table to show")
    parser.add_argument("-l", "--limit", help="Default limit for results (default: 100)", type=int)
    parser.add_argument("-c", "--cgi", help="Run as CGI script", action="store_true")
    parser.add_argument("-s", "--save-cache", help="Save Swagger cache", action="store_true")
    args = parser.parse_args()

    # Set up the database connection
    prepare(args.db, table=args.table, limit=args.limit)

    # Register blueprint and run app
    app = Flask(__name__)
    app.register_blueprint(sprocket)
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
