import csv
import json
import os
import requests

from copy import deepcopy
from flask import render_template, Response
from io import StringIO
from jinja2 import Environment, PackageLoader
from sqlalchemy.sql.expression import text as sql_text
from .lib import (
    exec_query,
    get_sql_columns,
    get_sql_tables,
    get_swagger_details,
    get_urls,
    parse_order_by,
    parse_where,
    SprocketError,
)

loader = PackageLoader("sprocket")
template_env = Environment(loader=loader)

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


def render_database_table(
    conn,
    table,
    request_args,
    base_url=None,
    default_limit=100,
    display_messages=None,
    hide_meta=True,
    ignore_cols=None,
    ignore_params=None,
    javascript=True,
    primary_key=None,
    show_help=False,
    standalone=True,
    use_view=True,
):
    """Get the SQL table for the Flask app. Either return the rendered HTML or a Response object
    containing TSV/CSV. Utilizes Flask request_args to construct the query to return results.
    :param conn: database connection
    :param table: table name
    :param request_args:
    :param default_limit: The max number of results to show per page, unless 'limit' is provided in
                        the query parameters.
    :param display_messages: dictionary containing messages to display as dismissible banners. The
                             dictionary can have the following keys: success, error, warn, info. The
                             values must be lists of string messages for that notification level.
    :param hide_meta: if True, hide any columns ending with '_meta'. These will be used to format
                      the cell value and (maybe) error message of the matching column.
    :param ignore_cols: list of columns of the SQL table to exclude from query/results.
    :param ignore_params: list of query parameters to exclude from URL.
    :param primary_key: The column name to use as the primary key for the table. This value will be
                        included as a hidden td in each table row with the HTML ID of pk{row_num}.
    :param show_help: if True, show descriptions for columns in single-row view.
                      This requires the 'column' table in the database.
    :param standalone: if True, include HTML headers & script in HTML output.
    :param use_view: if True, attempt to retrieve results from a '*_view' table which combines the
                      table and its conflict table."""
    tables = get_sql_tables(conn)
    if table not in tables:
        raise SprocketError(f"'{table}' is not a valid table in the database")
    table_cols = get_sql_columns(conn, table)

    descriptions = {}
    if show_help and "column" in tables:
        query = sql_text(
            """SELECT "column", description FROM "column"
               WHERE "table" = :table AND description IS NOT NULL"""
        )
        results = conn.execute(query, table=table)
        for res in results:
            descriptions[res["column"]] = res["description"]

    limit = request_args.get("limit", default_limit)
    try:
        limit = int(limit)
    except ValueError:
        raise SprocketError(f"'limit' ({limit}) must be an integer")

    offset = request_args.get("offset", "0")
    try:
        offset = int(offset)
    except ValueError:
        raise SprocketError(f"'offset' ({offset}) must be an integer")

    limit = limit + offset

    fmt = request_args.get("format", "html")
    if fmt not in ["tsv", "csv", "html"]:
        raise SprocketError(f"'format' must be 'tsv', 'csv', or 'html', not '{fmt}'")

    select = request_args.get("select")
    if select:
        select_cols = select.split(",")
        invalid_cols = list(set(select_cols) - set(table_cols))
        if invalid_cols:
            raise SprocketError(
                f"The following column(s) do not exist in '{table}' table: "
                + ", ".join(invalid_cols),
            )
        if hide_meta:
            # Add any necessary meta cols, since they don't appear in select filters
            select_cols.extend([f"{x}_meta" for x in select_cols if f"{x}_meta" in table_cols])
    else:
        select_cols = table_cols
    if ignore_cols:
        select_cols = [x for x in select_cols if x not in ignore_cols]

    where_statements = []
    for tc in table_cols:
        where = request_args.get(tc)
        if not where:
            continue
        try:
            stmt = parse_where(where, tc, postgres=str(conn.engine.url).startswith("postgres"))
        except ValueError as e:
            return SprocketError(e)
        where_statements.append(stmt)

    order_by = []
    order = request_args.get("order")
    if order:
        try:
            order_by = []
            for ob in parse_order_by(order):
                s = [f'"{ob["key"]}"']
                if ob["order"]:
                    s.append(ob["order"].upper())
                if ob["nulls"]:
                    s.append("NULLS " + ob["nulls"].upper())
                order_by.append(" ".join(s))
        except ValueError as e:
            return SprocketError(e)

    violations = request_args.get("violations")
    if violations:
        violations = violations.split(",")
        for v in violations:
            if v not in ["debug", "info", "warn", "error"]:
                return SprocketError(
                    f"'violations' contains invalid level '{v}' - "
                    "must be one of: debug, info, warn, error",
                )

    # Build & execute the query
    results = None
    if primary_key and fmt == "html" and primary_key not in select_cols:
        # We always need the primary key, even if it's hidden from select query param
        query_cols = deepcopy(select_cols)
        query_cols.insert(0, primary_key)
    else:
        query_cols = deepcopy(select_cols)
    if "row_number" in table_cols:
        query_cols.insert(0, "row_number")
    if use_view:
        results = exec_query(
            conn,
            table + "_view",
            columns=table_cols,
            select=query_cols,
            where_statements=where_statements,
            order_by=order_by,
            violations=violations,
        )
    if not results:
        results = exec_query(
            conn,
            table,
            columns=table_cols,
            select=query_cols,
            where_statements=where_statements,
            order_by=order_by,
            violations=violations,
        )

    # Return results based on format
    if fmt == "html":
        return render_html_table(
            results,
            table,
            request_args,
            base_url=base_url,
            columns=select_cols,
            default_limit=default_limit,
            descriptions=descriptions,
            display_messages=display_messages,
            ignore_params=ignore_params,
            javascript=javascript,
            primary_key=primary_key,
            standalone=standalone,
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
    writer.writerows(list(results)[offset: limit + offset])
    return Response(output.getvalue(), mimetype=mt)


def render_html_table(
    data,
    table,
    request_args,
    base_url=None,
    columns=None,
    conflict_prefix="row/",
    default_limit=100,
    descriptions=None,
    display_messages=None,
    hide_meta=True,
    ignore_params=None,
    include_expand=True,
    javascript=True,
    primary_key=None,
    standalone=True,
    total=None,
):
    """Render the results as an HTML table.

    :param data:
    :param table:
    :param request_args:
    :param base_url:
    :param columns: Optional list of column names to display as headers of the HTML table.
                    If not provided, the keys of the first element of data are used as headers.
    :param conflict_prefix: Prefix to use for row_number when primary_key is provided and there is a
                            primary_key conflict, e.g. row/32.
    :param default_limit:
    :param descriptions:
    :param display_messages:
    :param hide_meta:
    :param ignore_params: list of query parameters to exclude from URLs
    :param include_expand:
    :param primary_key: The column name to use as the primary key for the table. This value will be
                        included as a hidden td in each table row with the HTML ID of pk{row_num}.
    :param standalone:
    :param total:
    :return:
    """
    if columns:
        header_names = columns
    else:
        header_names = list(data[0].keys())

    if "select" in request_args and primary_key:
        # Maybe filter the header names to get rid of cols we hide in the row
        select_cols = request_args["select"].split(",")
        if "*" not in select_cols:
            header_names = select_cols

    # Clean up null values and add styles
    results = []
    for res in data:
        values = {}
        for k, v in res.items():
            style = None
            if not v:
                v = ""
                style = "null"
            values[k] = {"value": str(v), "style": style, "message": None, "header": k}
        results.append(values)

    if hide_meta:
        # exclude *_meta columns from display and use the values to render cell styles
        meta_names = [x for x in header_names if x.endswith("_meta")]
        header_names = [x for x in header_names if x not in meta_names]
        # also update columns for selections
        if columns:
            columns = [x for x in columns if not x.endswith("_meta")]
        # iter through results and update
        res_updated = []
        for res in results:
            for m in meta_names:
                # Get the metadata as JSON
                meta = res[m]["value"]
                del res[m]
                if not meta:
                    continue
                metadata = json.loads(meta)

                if metadata.get("valid") and not metadata.get("nulltype"):
                    # Cell is not a null & is valid, nothing to style or change
                    continue

                # This is the name of the column we are editing
                value_col = m[:-5]
                # Set the value to what is given in the JSON (as "value")
                # unless there is a primary key conflict, in which case use conflict format
                pk_value = None
                if primary_key and value_col == primary_key and "row_number" in res:
                    # Determine if the there is a conflict
                    for msg in metadata["messages"]:
                        if msg["rule"] == "key:primary":
                            pk_value = f"{conflict_prefix}{res['row_number']['value']}"
                if not pk_value:
                    pk_value = metadata["value"]
                res[value_col]["conflict_key"] = pk_value
                res[value_col]["value"] = metadata["value"]
                if "nulltype" in metadata:
                    # Set null style and go to next
                    res[value_col]["style"] = "null"
                    continue

                # Use a number for violation level to make sure the "worst" violation is displayed
                violation_level = -1
                messages = []
                if "messages" in metadata:
                    for msg in metadata["messages"]:
                        lvl = msg["level"]
                        messages.append(msg["message"])
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
                if len(messages) > 1:
                    messages = [f"({i}) {msg}" for i, msg in enumerate(messages, 1)]
                res[value_col]["message"] = "<br>".join(messages).replace('"', "&quot;")
            res_updated.append(res)
        results = res_updated

    offset = int(request_args.get("offset", "0"))
    limit = int(request_args.get("limit", default_limit))

    if not total:
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

    # Set the options for filtering - only if we're showing options
    headers = {}
    for h in header_names:
        fltr = request_args.get(h)
        if not fltr:
            headers[h] = {"options": FILTER_OPTS, "has_selected": False}
            continue
        cur_options = deepcopy(FILTER_OPTS)
        # Make sure to split correctly in case constraint has a dot
        # The only time the filter has two dots is when not is used
        if fltr.startswith("not"):
            opt = ".".join(fltr.split(".", 2)[:2])
            val = fltr.split(".", 2)[2]
        else:
            opt = fltr.split(".", 1)[0]
            val = fltr.split(".", 1)[1]
        cur_options[opt]["selected"] = True
        headers[h] = {"options": cur_options, "const": val}

    # Set the options for violation filtering
    violations = request_args.get("violations", "").split(",")

    if not base_url:
        base_url = "./" + table
    urls = get_urls(
        base_url, request_args, total, ignore_params=ignore_params, offset=offset, limit=limit
    )

    # Get the columns we're sorting by and put into appropriate list so we know which btn to show
    order = request_args.get("order")
    sort_asc = []
    sort_desc = []
    if order:
        for ob in parse_order_by(order):
            if ob["order"] == "asc":
                sort_asc.append(ob["key"])
            else:
                sort_desc.append(ob["key"])

    render_args = {
        "headers": headers,
        "include_expand": include_expand,
        "javascript": javascript,
        "limit": limit,
        "messages": display_messages,
        "offset": offset,
        "options": options,
        "select": columns,
        "sort_asc": sort_asc,
        "sort_desc": sort_desc,
        "standalone": standalone,
        "title": table,
        "total": total,
        "urls": urls,
        "violations": violations,
    }
    if limit == 1 or total == 1:
        render_args["descriptions"] = descriptions
        render_args["row"] = results[0]
        template = "vertical.html"
    else:
        # Create the row to pass to template, to know what to display (hidden vs visible)
        display_rows = []
        for row in results:
            # Find the values, maybe delete the item if it shouldn't be included in display
            if primary_key:
                # Key value is either the conflict key or just the value of the primary key col
                key_val = row[primary_key].get("conflict_key", row[primary_key]["value"])
                if primary_key not in header_names:
                    del row[primary_key]
                    if primary_key + "_meta" in row:
                        del row[primary_key + "_meta"]
            else:
                key_val = None
            display_rows.append({"cells": row, "row_key": key_val})

        render_args["rows"] = display_rows
        template = "horizontal.html"
    t = template_env.get_template(template)
    return t.render(**render_args)


def get_value_from_row(row, col):
    for cell in row:
        if cell["header"] == col:
            return cell["value"]
    return None


def render_swagger_table(
    swagger_url, table, request_args, default_limit=100, javascript=True, standalone=False, swagger_cache=".swagger"
):
    """Get the SQL table for the Flask app from a Swagger endpoint. Either return the rendered HTML
    or a Response object containing TSV/CSV. Uses query parameters (request_args) to construct query
    to return results.

    :param swagger_url: URL to remote database (Swagger)
    :param table: table name
    :param request_args:
    :param default_limit: if limit parameter is not provided, default number of results to show
    :param standalone: if True, include HTML headers & script in HTML output.
    :param swagger_cache: directory to store Swagger details
    :return: rendered HTML or Response containing table to download
    """
    if not os.path.exists(swagger_cache):
        os.mkdir(swagger_cache)

    # Create a URL to get JSON from
    url = swagger_url + "/" + table
    swagger_request_args = []
    get_all_columns = False

    # Parse args and create request
    fmt = None
    has_limit = False
    for arg, value in request_args.items():
        if arg == "select":
            get_all_columns = True
        if arg == "limit":
            has_limit = True
        if arg == "violations":
            continue
        if arg == "format":
            fmt = value
            continue
        swagger_request_args.append(f"{arg}={value}")
    if not has_limit:
        # We always want to have the limit
        swagger_request_args.append(f"limit={default_limit}")

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
        swagger_url, table, data, get_all_columns=get_all_columns, swagger_cache=swagger_cache
    )
    return render_html_table(
        data,
        table,
        request_args,
        columns=columns,
        total=total,
        default_limit=default_limit,
        javascript=javascript,
        standalone=standalone,
    )


def render_tsv_table(data, fmt="tsv"):
    headers = data[0].keys()
    output = StringIO()
    sep = "\t"
    if fmt == "csv":
        sep = ","
    writer = csv.DictWriter(output, delimiter=sep, fieldnames=list(headers), lineterminator="\n")
    writer.writeheader()
    writer.writerows(list(data))
    return output.getvalue()
