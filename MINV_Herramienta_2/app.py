"""Servidor de laboratorio. Solo se expone en localhost mediante Compose."""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs
import core


def make_server(role, mode, path, port, inventory_url='', host='0.0.0.0'):
    core.init(path)
    processing = threading.Semaphore(1)  # Un trabajador de inventario por prototipo.
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, code, value):
            body = json.dumps(value).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # El timeout del cliente no revierte la operación.

        def do_GET(self):
            url = urlsplit(self.path)
            if url.path == '/health':
                return self.respond(200, {'role': role, 'mode': mode})
            query = parse_qs(url.query)
            if url.path in ('/records', '/counts') and 'run_id' in query:
                return self.respond(200, core.report(path, query['run_id'][0], role, url.path == '/counts'))
            return self.respond(404, {'error': 'Ruta inexistente'})

        def do_POST(self):
            try:
                length = int(self.headers.get('Content-Length', 0))
                if not 0 < length <= 4096:
                    raise ValueError('Tamaño de solicitud inválido')
                payload = json.loads(self.rfile.read(length))
                if self.path == '/orders' and role == 'orders':
                    order = core.create_order(path, payload, mode)
                    if order['status'] == 'CONFIRMADO':
                        return self.respond(200, order)
                    if mode == 'outbox':
                        return self.respond(202, order)
                    try:
                        core.request(inventory_url + '/reserve', core.validate(payload))
                    except Exception as error:
                        # Estado incierto: inventario puede completar tras un timeout.
                        # No se inventa un FALLO de negocio ni se confirma éxito.
                        return self.respond(503, {'id': payload['id'], 'status': 'PENDIENTE',
                                                  'error': type(error).__name__})
                    return self.respond(200, core.complete(path, payload['id']))
                if self.path == '/reserve' and role == 'inventory':
                    with processing:
                        time.sleep(float(os.environ.get('PROCESSING_MS', '10')) / 1000)
                        result = core.reserve(path, payload)
                    return self.respond(200, result)
                if self.path == '/complete' and role == 'orders' and mode == 'outbox':
                    return self.respond(200, core.complete(path, payload['id']))
                return self.respond(404, {'error': 'Ruta inexistente'})
            except (ValueError, KeyError, TypeError) as error:
                return self.respond(400, {'error': str(error)})
            except Exception as error:
                return self.respond(500, {'error': type(error).__name__})
    return ThreadingHTTPServer((host, port), Handler)


if __name__ == '__main__':
    role = os.environ.get('ROLE', 'orders')
    mode = os.environ.get('MODE', 'direct')
    path = os.environ.get('DB_PATH', '/data/service.sqlite')
    server = make_server(role, mode, path, int(os.environ.get('PORT', '8000')),
                         os.environ.get('INVENTORY_URL', ''))
    if mode == 'outbox':
        import messaging
        target = messaging.relay if role == 'orders' else messaging.consumer
        threading.Thread(target=target, args=(path,), daemon=True).start()
    print(f'{role}/{mode}: servidor listo', flush=True)
    server.serve_forever()
