import requests

from lark.exceptions import UnexpectedInput
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Dict, List, Optional, Tuple
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
    """Get a list of columns from a table.

    :param conn: local database connection
    :param table: table name to get columns of
    :return: list of columns
    """
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
    """Get a list of tables from a database.

    :param conn: local database connection
    :return list of SQL tables
    """
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


def get_swagger_tables(url: str) -> List[str]:
    """Use the URL (Swagger endpoint) to get a list of tables.

    :param url: Swagger endpoint
    :return list of tables
    """
    try:
        r = requests.get(url, verify=False)
    except requests.exceptions.MissingSchema:
        raise SprocketError("Malformed endpoint URL: " + url)
    except ConnectionError:
        raise SprocketError("Unable to connect to endpoint: " + url)
    data = r.json()
    try:
        return [
            path[1:] for path, details in data["paths"].items() if path != "/" and "get" in details
        ]
    except ValueError:
        raise SprocketError("Malformed Swagger data from endpoint: " + url)


def get_urls(
    base_url: str,
    request_args: dict,
    total_results: int,
    ignore_params: list = None,
    offset: int = 0,
    limit: int = 100,
) -> Dict[str, str]:
    """Use the offset and limit to create important URLs for pagination in the HTML table output.
    This is a dict with 5 keys:
    - first: URL to go to first page
    - prev: URL to go to previous page
    - next: URL to go to next page
    - last: URL to go to last page
    - this: URL for the current location

    :param base_url: The base URL for this page without query parameters
    :param request_args: dict of HTTP request args (Flask request.args)
    :param total_results: number of total results, used to calculate the offset for last page
    :param ignore_params: list of query parameters to exclude from URLs
    :param offset: current 'location' (where to begin displaying results)
    :param limit: number of results to display per page
    :return: dict of URLs
    """
    if not ignore_params:
        ignore_params = []
    # Get URLs for "previous" and "next" links
    first_url = None
    prev_url = None
    next_url = None
    last_url = None
    if offset > 0:
        # Only include "previous" and "first" if we aren't at the beginning
        prev_args = request_args.copy()
        prev_offset = offset - limit
        if prev_offset < 0:
            prev_offset = 0
        prev_args["offset"] = prev_offset
        prev_query = [f"{k}={v}" for k, v in prev_args.items() if k not in ignore_params]
        prev_url = base_url
        if prev_query:
            prev_url += "?" + "&".join(prev_query)

        del prev_args["offset"]
        first_query = [f"{k}={v}" for k, v in prev_args.items() if k not in ignore_params]
        first_url = base_url
        if first_query:
            first_url += "?" + "&".join(first_query)
    if limit + offset < total_results:
        # Only include "next" and "last" link if we aren't at the end
        next_args = request_args.copy()
        next_args["offset"] = limit + offset
        next_query = [f"{k}={v}" for k, v in next_args.items() if k not in ignore_params]
        next_url = base_url
        if next_query:
            next_url += "?" + "&".join(next_query)

        next_args["offset"] = total_results - limit
        last_query = [f"{k}={v}" for k, v in next_args.items() if k not in ignore_params]
        last_url = base_url
        if last_query:
            last_url += "?" + "&".join(last_query)

    # Current URL is used for download links (always include ?)
    this_query = [f"{k}={v}" for k, v in request_args.items() if k not in ignore_params]
    this_url = base_url + "?"
    if this_query:
        this_url += "&".join(this_query)
    return {
        "first": first_url,
        "prev": prev_url,
        "next": next_url,
        "last": last_url,
        "this": this_url,
    }


def parse_order_by(order: str) -> List[dict]:
    """Return a list of columns to order by from a string passed through query parameters. The
    format is modeled on https://postgrest.org/en/latest/api.html#ordering. Each column is
    represented as a dict with:
    - key: column name
    - order: direction to sort (asc or desc)
    - nulls: where to place nulls (first or last)

    :param order: order string from query parameters
    :return: list of order-specification dicts
    """
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


def parse_where(where: str, column, postgres=False) -> Tuple[str, str]:
    """Create a where clause by parsing the horizontal filtering condition.
    The WHERE is a tuple containing the operator (e.g., LIKE) and the constraint (e.g., "foo", or
    None for some like NULL) so that we can use the constraints in parameterized queries.

    :param where: where condition (operator + constraint modeled on
                  https://postgrest.org/en/latest/api.html#operators)
    :param column: column to apply filter
    :param postgres: if True, use Postgres syntax which includes ILIKE
    :return: a tuple (where statement, constraint)"""
    # Parse using Lark grammar
    try:
        parsed = PARSER.parse(where)
    except UnexpectedInput:
        raise SprocketError(f"Invalid filter constraint for column '{column}': {where}")
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
        raise SprocketError(f"The constraint for '{operator}' must be a single value, not a list")
    elif operator == "in" and not isinstance(constraint, list):
        raise SprocketError("The constraint for 'in' must be a list")

    # Set the SQL operator
    col_name = f'"{column}"'
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
    elif operator == "like":
        query_op = "LIKE"
        if "%" not in constraint:
            constraint = f"%%{constraint}%%"
        else:
            constraint = constraint.replace("%", "%%")
    elif operator == "ilike":
        query_op = "ILIKE"
        if "%" not in constraint:
            constraint = f"%%{constraint}%%"
        else:
            constraint = constraint.replace("%", "%%")
        if not postgres:
            query_op = "LIKE"
            col_name = f'lower("{column}")'
    else:
        query_op = operator.upper()
    return statement + f"{col_name} {query_op}", constraint


class SprocketError(RuntimeError):
    """Base class for any runtime exceptions thrown in sprocket code."""
