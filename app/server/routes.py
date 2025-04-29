import os

from knbase.sqlite3_pool import enter_thread_pool, exit_thread_pool
from flask import (
  request,
  jsonify,
  send_file,
  send_from_directory,
  Flask,
  Response,
)

from .service import Service

def routes(app: Flask, service: Service) -> None:

  @app.route("/static/<file_name>")
  def get_static_file(file_name: str):
    root_path = os.path.join(__file__, "..", "..", "browser.lib")
    root_path = os.path.abspath(root_path)
    dir_path = os.path.join(root_path, "dist")
    dir_path = os.path.abspath(dir_path)
    file_path = os.path.join(dir_path, file_name)
    file_path = os.path.abspath(file_path)

    if os.path.exists(file_path):
      return send_from_directory(dir_path, file_name)

    dir_path = os.path.join(root_path, "static")
    dir_path = os.path.abspath(dir_path)

    return send_from_directory(dir_path, file_name)

  @app.route("/api/query", methods=["GET"])
  def get_query():
    query = request.args.get("query", "")
    results_limit = request.args.get("resultsLimit", "")
    if query == "":
      raise ValueError("Invalid query")
    if results_limit == "":
      raise ValueError("Invalid resultsLimit")

    result = service.ref.query(
      text=query,
      results_limit=int(results_limit),
    )
    return jsonify(result)

  @app.route("/api/scanning", methods=["GET"])
  def get_scanning():
    return Response(
      service.gen_scanning_sse_lines(),
      content_type="text/event-stream",
    )

  @app.route("/api/scanning", methods=["DELETE"])
  def delete_scanning():
    service.interrupt_scanning()
    return jsonify(None), 204

  @app.route("/api/scanning", methods=["POST"])
  def post_scanning():
    service.start_scanning()
    return jsonify(None), 201

  @app.route("/api/sources", methods=["GET"])
  def get_sources():
    return jsonify([
      {
        "name": name,
        "path": path,
      }
      for name, path in service.sources.items()
    ])

  @app.route("/api/sources", methods=["PUT"])
  def put_sources():
    body = request.json
    if not isinstance(body, dict):
      raise ValueError("Invalid body")

    name = body.get("name", None)
    path = body.get("path", None)

    if not isinstance(name, str):
      raise ValueError("Invalid name")
    if not isinstance(path, str):
      raise ValueError("Invalid path")

    service.sources.put(name, path)
    return jsonify({
      "name": name,
      "path": path,
    })

  @app.route("/api/sources", methods=["DELETE"])
  def delete_sources():
    name = request.args.get("name", "")
    if name == "":
      raise ValueError("Invalid name")
    service.sources.remove(name)
    return jsonify(None), 204

  @app.route("/files/<scope>/<path:path>", methods=["GET"])
  def open_pdf_file(scope: str, path: str):
    device_path = service.ref.device_path(scope, path)
    if device_path is None:
      return jsonify({ "error": "Not found" }), 404

    return send_file(
      path_or_file=device_path,
      conditional=True, # 304 if needed
    )

  @app.before_request
  def before_request():
    enter_thread_pool()

  @app.teardown_request
  def teardown_request(response):
    exit_thread_pool()
    return response

  @app.errorhandler(404)
  def page_not_found(_):
    mimetypes = request.accept_mimetypes
    if mimetypes.accept_json and not mimetypes.accept_html:
      return jsonify({ "error": "Not found" }), 404

    path = os.path.join(__file__, "..", "..", "browser.lib", "index.html")
    path = os.path.abspath(path)
    return send_file(path, mimetype="text/html")

  @app.errorhandler(500)
  def internal_server_error(e):
    return jsonify({
      "error": "Internal server error",
      "description": str(e),
    }), 500
