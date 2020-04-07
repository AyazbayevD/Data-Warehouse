import os

from flask import Flask, request, abort, jsonify, send_from_directory

api = Flask(__name__)
UPLOAD_DIRECTORY = "/home/iskander/PycharmProjects/Data-Warehouse/temp_files"


@api.route("/files/<path>")
def get_file(path):
    """Download a file."""
    return send_from_directory(UPLOAD_DIRECTORY, path, as_attachment=True)


@api.route("/files/<filename>", methods=["POST"])
def post_file(filename):
    """Upload a file."""

    if "/" in filename:
        # Return 400 BAD REQUEST
        abort(400, "no subdirectories directories allowed")

    with open(os.path.join(UPLOAD_DIRECTORY, filename), "wb") as fp:
        fp.write(request.data)

    # Return 201 CREATED
    return "", 201


@api.route('/')
def hello():
    print('Hello')


if __name__ == "__main__":
    api.run(debug=True, port=2036)
