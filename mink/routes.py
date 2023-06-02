"""Collection of general routes."""

import os

import yaml
from flask import Blueprint
from flask import current_app as app
from flask import jsonify, redirect, render_template, send_from_directory, url_for


bp = Blueprint("general", __name__)


@bp.route("/")
def hello():
    """Redirect to /api_doc."""
    return redirect(app.config.get("MINK_URL") + url_for("general.api_doc"))


@bp.route("/api-spec")
def api_spec():
    """Return open API specification in json."""
    host = app.config.get("MINK_URL")
    spec_file = os.path.join(app.static_folder, "oas.yaml")
    with open(spec_file, encoding="UTF-8") as f:
        strspec = f.read()
        # Replace {{host}} in examples with real URL
        strspec = strspec.replace("{{host}}", host)
        return jsonify(yaml.safe_load(strspec))


@bp.route("/api-doc")
def api_doc():
    """Render HTML API documentation."""
    return render_template("apidoc.html",
                           title="Mink API documentation",
                           favicon=app.config.get("MINK_URL") + url_for("static", filename="favicon.ico"),
                           logo=app.config.get("MINK_URL") + url_for("static", filename="mink.svg"),
                           spec_url=app.config.get("MINK_URL") + url_for("general.api_spec")
                           )


@bp.route("/developers-guide")
def developers_guide():
    """Render docsify HTML with the developer's guide."""
    return render_template("docsify.html",
                           favicon=app.config.get("MINK_URL") + url_for("static", filename="favicon.ico"))


@bp.route("/developers-guide/<path:path>")
def developers_guide_files(path):
    """Serve sub pages to the developer's guide needed by docsify."""
    return send_from_directory("templates", path)


# @bp.route("/routes")
# def routes():
#     """Show available routes."""
#     routes = [str(rule) for rule in app.url_map.iter_rules()]
#     return utils.response("Listing available routes", routes=routes)


# @bp.route("/status-codes")
# def status_codes():
#     """Show existing job status codes."""
#     from mink.jobs import Status
#     status_codes = []
#     for s in Status:
#         status_codes.append({"code": s._value_, "name": s.name, "description": s.desc})
#     return utils.response("Listing existing job status codes", status_codes=status_codes)
