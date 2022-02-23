import csv
import json
import os
import requests

from copy import deepcopy
from flask import abort, render_template, request, Response
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
    default_limit=100,
    display_messages=None,
    hide_meta=True,
    show_help=False,
    standalone=True,
):
    """Get the SQL table for the Flask app. Either return the rendered HTML or a Response object
    containing TSV/CSV. Utilizes Flask request.args.

    :param conn: database connection
    :param table: table name
    :param default_limit: The max number of results to show per page, unless 'limit' is provided in
                        the query parameters.
    :param display_messages: dictionary containing messages to display as dismissible banners. The
                             dictionary can have the following keys: success, error, warn, info. The
                             values must be lists of string messages for that notification level.
    :param hide_meta: if True, hide any columns ending with '_meta'. These will be used to format
                      the cell value and (maybe) error message of the matching column.
    :param show_help: if True, show descriptions for columns in single-row view.
                      This requires the 'column' table in the database.
    :param standalone: if False, do not include HTML headers & script."""
    tables = get_sql_tables(conn)
    if table not in tables:
        return abort(422, f"'{table}' is not a valid table in the database")
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

    limit = request.args.get("limit", default_limit)
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
                    f"'violations' contains invalid level '{v}' - "
                    "must be one of: debug, info, warn, error",
                )

    # Build & execute the query
    results = exec_query(
        conn,
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
            results,
            table,
            table_cols,
            request.args,
            default_limit=default_limit,
            descriptions=descriptions,
            display_messages=display_messages,
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
    writer.writerows(list(results)[offset : limit + offset])
    return Response(output.getvalue(), mimetype=mt)


def render_html_table(
    data,
    table,
    columns,
    request_args,
    base_url=None,
    default_limit=100,
    descriptions=None,
    display_messages=None,
    hidden=None,
    hide_meta=True,
    include_expand=True,
    show_options=True,
    standalone=True,
    total=None,
):
    """Render the results as an HTML table."""
    header_names = list(data[0].keys())

    # Clean up null values and add styles
    results = []
    for res in data:
        values = {}
        for k, v in res.items():
            style = None
            if not v:
                v = ""
                style = "null"
            values[k] = {"value": v, "style": style, "message": None, "header": k}
        results.append(values)

    if hide_meta:
        # exclude *_meta columns from display and use the values to render cell styles
        meta_names = [x for x in header_names if x.endswith("_meta")]
        header_names = [x for x in header_names if x not in meta_names]
        # also update columns for selections
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
                metadata = json.loads(meta[5:-1])

                if metadata.get("valid") and not metadata.get("nulltype"):
                    # Cell is not a null & is valid, nothing to style or change
                    continue

                # This is the name of the column we are editing
                value_col = m[:-5]
                # Set the value to what is given in the JSON (as "value")
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
        opt = fltr.rsplit(".", 1)[0]
        val = fltr.rsplit(".", 1)[1]
        cur_options[opt]["selected"] = True
        headers[h] = {"options": cur_options, "const": val}

    # Set the options for violation filtering
    violations = request_args.get("violations", "").split(",")

    if not base_url:
        base_url = "./" + table
    prev_url, next_url, this_url = get_urls(base_url, request_args, total, offset=offset, limit=limit)

    hidden_args = {}
    if hidden:
        for h in hidden:
            hidden_args[h] = request_args.get(h)

    import logging
    logging.error(display_messages)
    render_args = {
        "title": table,
        "select": columns,
        "options": options,
        "violations": violations,
        "offset": offset,
        "headers": headers,
        "hidden": hidden_args,
        "total": total,
        "limit": limit,
        "this_url": this_url,
        "prev_url": prev_url,
        "next_url": next_url,
        "include_expand": include_expand,
        "show_options": show_options,
        "standalone": standalone,
        "messages": display_messages,
    }
    if limit == 1 or total == 1:
        row = {}
        i = 0
        for h in headers:
            row[h] = results[0][i]
            i += 1
        render_args["row"] = row
        render_args["descriptions"] = descriptions
        template = "vertical.html"
    else:
        render_args["rows"] = results
        render_args["headers"] = headers
        template = "horizontal.html"
    t = template_env.get_template(template)
    return t.render(**render_args)


def render_swagger_table(swagger_url, table, default_limit=100, swagger_cache=".swagger"):
    """Get the SQL table for the Flask app from a Swagger endpoint. Either return the rendered HTML
    or a Response object containing TSV/CSV. Utilizes Flask request.args.

    :param swagger_url: URL to remote database (Swagger)
    :param table: table name
    :param default_limit: if limit parameter is not provided, default number of results to show
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
    for arg, value in request.args.items():
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
        data, table, columns, request.args, total=total, default_limit=default_limit
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
