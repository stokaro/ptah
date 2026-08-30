"""Deterministic OpenAI-compatible embedding endpoint for the docs tutorial."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.send_response(204)
        self.end_headers()

    def do_POST(self):
        if self.path != "/v1/embeddings":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length))
        inputs = request.get("input", [])
        if isinstance(inputs, str):
            inputs = [inputs]

        data = []
        for index, value in enumerate(inputs):
            size = len(value.encode("utf-8"))
            data.append({
                "object": "embedding",
                "index": index,
                "embedding": [float(size + offset) for offset in range(4)],
            })

        response = json.dumps({
            "object": "list",
            "model": request.get("model", "ptah-docs-stub"),
            "data": data,
            "usage": {"prompt_tokens": len(inputs), "total_tokens": len(inputs) * 2},
        }).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format, *args):
        return


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
