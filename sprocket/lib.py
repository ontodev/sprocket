import csv
import os
import requests

from collections import defaultdict
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Tuple, List, Optional
from .grammar import PARSER, SprocketTransformer


def exec_query(
    conn: Connection,
    table: str,
    columns: Optional[List[str]] = None,
    select: List[str] = None,
    where_statements: List[Tuple] = None,
    order_by: List[str] = None,
    violations: List[str] = None,
) -> List[dict]:
    """
    :param conn: database connection to query
    :param table: name of the table to query
    :param columns: list of all columns in table (required for meta violation filtering)
    :param select: columns to select (default: *)
    :param where_statements: WHERE constraints for the query as a list of tuples
                             (operator, constraint)
    :param order_by: list of columns to order results by
    :param violations: violation level(s) to filter meta columns by (requires columns as well)
    :return: query results
    """
    if not select:
        select = ["*"]
    query = "SELECT "
    select_strs = []
    for s in select:
        if s == "*":
            select_strs.append("*")
        else:
            select_strs.append(f'"{s}"')
    query += ", ".join(select_strs)
    query += f' FROM "{table}"'
    const_dict = {}
    # Add keys for any where statements using user input values
    if where_statements:
        n = 0
        expanded_statements = []
        for ws, constraint in where_statements:
            if constraint is None:
                # Do not use not constraint in case int 0 is provided
                expanded_statements.append(ws)
                continue
            k = f"const{n}"
            ws += f" :{k}"
            const_dict[k] = constraint
            expanded_statements.append(ws)
            n += 1
        query += " WHERE " + " AND ".join(expanded_statements)
    if violations and columns:
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
                likes.append(f'trim("{m}") LIKE \'%"level":"{v}"%\'')
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
    return conn.execute(query, const_dict).fetchall()


def get_sql_columns(conn: Connection, table: str) -> List[str]:
    """Get a list of columns from a table."""
    # Check for required columns
    if str(conn.engine.url).startswith("sqlite"):
        res = conn.execute(f"PRAGMA table_info('{table}')")
    else:
        res = conn.execute(
            f"""SELECT column_name AS name FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_NAME = '{table}';"""
        )
    return [x["name"] for x in res]


def get_sql_tables(conn: Connection) -> List[str]:
    """Get a list of tables from a database."""
    if str(conn.engine.url).startswith("sqlite"):
        res = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_conflict';"
        )
    else:
        res = conn.execute(
            """SELECT table_name AS name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name NOT LIKE '%_conflict';"""
        )
    return [x["name"] for x in res]


def get_swagger_details(url, table, data, get_all_columns=False, swagger_cache=None):
    """Get a list of columns for a table from Swagger,
    checking first if we've cached the columns."""
    # Check for columns in the cache file
    table_columns = defaultdict(list)
    columns = None
    columns_file = None
    if swagger_cache:
        columns_file = os.path.join(swagger_cache, "columns.tsv")
        if os.path.exists(columns_file):
            with open(columns_file, "r") as f:
                reader = csv.reader(f, delimiter="\t")
                for row in reader:
                    if row[0] not in table_columns:
                        table_columns[row[0]] = list()
                    table_columns[row[0]].append(row[1])
        columns = table_columns.get(table)

    total = None
    totals_file = None
    totals_rows = []
    if swagger_cache:
        totals_file = os.path.join(swagger_cache, "totals.tsv")
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
                f"{url}/{table}?limit=1", headers={"Prefer": "count=estimated"}, verify=False
            )
            data2 = r.json()[0]
            columns = list(data2.keys())
            total = int(r.headers["Content-Range"].split("/")[1])
        else:
            # We already have the total and we have all the columns in the data - no need to hit API
            columns = list(data[0].keys())

        if swagger_cache:
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


def get_swagger_tables(url):
    # TODO: error handling
    r = requests.get(url, verify=False)
    data = r.json()
    return [path[1:] for path, details in data["paths"].items() if path != "/" and "get" in details]


def get_urls(
    base_url, request_args, total_results, ignore_params=None, offset=0, limit=100
) -> Tuple[str, str, str]:
    if not ignore_params:
        ignore_params = []
    # Get URLs for "previous" and "next" links
    prev_url = None
    next_url = None
    if offset > 0:
        # Only include "previous" link if we aren't at the beginning
        prev_args = request_args.copy()
        prev_offset = offset - limit
        if prev_offset < 0:
            prev_offset = 0
        prev_args["offset"] = prev_offset
        prev_query = [f"{k}={v}" for k, v in prev_args.items() if k not in ignore_params]
        prev_url = base_url + "?" + "&".join(prev_query)
    if limit + offset < total_results:
        # Only include "next" link if we aren't at the end
        next_args = request_args.copy()
        next_args["offset"] = limit + offset
        next_query = [f"{k}={v}" for k, v in next_args.items() if k not in ignore_params]
        next_url = base_url + "?" + "&".join(next_query)

    # Current URL is used for download links
    this_url = (
        base_url
        + "?"
        + "&".join([f"{k}={v}" for k, v in request_args.items() if k not in ignore_params])
    )
    return prev_url, next_url, this_url


def parse_order_by(order) -> List[dict]:
    order_by = []
    for itm in order.split(","):
        attrs = itm.split(".")
        if len(attrs) == 1:
            order_by.append({"key": attrs[0], "order": "asc", "nulls": "last"})
        elif len(attrs) == 2:
            if attrs[1] == "nullsfirst":
                order_by.append({"key": attrs[0], "order": "asc", "nulls": "first"})
            elif attrs[1] == "nullslast":
                order_by.append({"key": attrs[0], "order": "asc", "nulls": "last"})
            elif attrs[1] == "asc":
                order_by.append({"key": attrs[0], "order": "asc", "nulls": "last"})
            elif attrs[1] == "desc":
                order_by.append({"key": attrs[0], "order": "desc", "nulls": "last"})
            else:
                raise ValueError("Unknown order qualifier: " + attrs[1])
        elif len(attrs) == 3:
            d = {"key": attrs[0], "order": "asc", "nulls": "last"}
            if attrs[1] == "desc":
                d["order"] = "desc"
            elif attrs[1] != "asc":
                raise ValueError(
                    "Second 'order' modifier must be either 'asc' or 'desc', not " + attrs[1]
                )
            if attrs[2] == "nullsfirst":
                d["nulls"] = "first"
            elif attrs[2] != "nullslast":
                raise ValueError(
                    "Third 'order' modifier must be either 'nullsfirst' or 'nullslast', not "
                    + attrs[2]
                )
            order_by.append(d)
    return order_by


def parse_where(where, column) -> Tuple[str, str]:
    """Create a where clause by parsing the horizontal filtering condition.
    The WHERE is a tuple containing the operator (e.g., LIKE) and the constraint (e.g., "foo", or
    None for some like NULL) so that we can use the constraints in parameterized queries.

    :param where:
    :param column:
    :return: a tuple containing a tuple (where statement, constraint) and an error message
            (None on success)"""
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
        raise ValueError(f"The constraint for '{operator}' must be a single value, not a list")
    elif operator == "in" and not isinstance(constraint, list):
        raise ValueError("The constraint for 'in' must be a list")

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
    return statement + f'"{column}" {query_op}', constraint
