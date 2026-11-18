"""Persistencia y reglas comunes. No necesita dependencias externas."""
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import Request, urlopen


@contextmanager
def db(path):
    con = sqlite3.connect(path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def init(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with db(path) as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript('''
        CREATE TABLE IF NOT EXISTS orders (
          id TEXT PRIMARY KEY, run_id TEXT NOT NULL, quantity INTEGER NOT NULL,
          created REAL NOT NULL, completed REAL, status TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS orders_run ON orders(run_id);
        CREATE TABLE IF NOT EXISTS outbox (
          id TEXT PRIMARY KEY REFERENCES orders(id), payload TEXT NOT NULL,
          sent REAL, attempts INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS reservations (
          id TEXT PRIMARY KEY, run_id TEXT NOT NULL, quantity INTEGER NOT NULL,
          reserved REAL NOT NULL, deliveries INTEGER NOT NULL DEFAULT 1);
        ''')


def validate(p):
    if not isinstance(p, dict):
        raise ValueError('El cuerpo debe ser un objeto JSON')
    for k in ('id', 'run_id'):
        if not isinstance(p.get(k), str) or not 1 <= len(p[k]) <= 120:
            raise ValueError('id y run_id deben contener entre 1 y 120 caracteres')
    if type(p.get('quantity')) is not int or not 1 <= p['quantity'] <= 100:
        raise ValueError('quantity debe ser un entero entre 1 y 100')
    return {k: p[k] for k in ('id', 'run_id', 'quantity')}


def create_order(path, payload, mode):
    p = validate(payload)
    with db(path) as c:
        c.execute('BEGIN IMMEDIATE')
        old = c.execute('SELECT * FROM orders WHERE id=?', (p['id'],)).fetchone()
        if old:
            if old['run_id'] != p['run_id'] or old['quantity'] != p['quantity']:
                raise ValueError('Identificador reutilizado con otros datos')
            return dict(old)
        c.execute('INSERT INTO orders VALUES (?,?,?,?,NULL,?)',
                  (p['id'], p['run_id'], p['quantity'], time.time(), 'PENDIENTE'))
        if mode == 'outbox':
            # Pedido y evento pertenecen a la MISMA transacción SQLite.
            c.execute('INSERT INTO outbox(id,payload) VALUES (?,?)',
                      (p['id'], json.dumps(p)))
    return get_order(path, p['id'])


def reserve(path, payload):
    p = validate(payload)
    with db(path) as c:
        c.execute('BEGIN IMMEDIATE')
        old = c.execute('SELECT * FROM reservations WHERE id=?', (p['id'],)).fetchone()
        if old:
            if old['quantity'] != p['quantity'] or old['run_id'] != p['run_id']:
                raise ValueError('Reserva incompatible con el identificador')
            c.execute('UPDATE reservations SET deliveries=deliveries+1 WHERE id=?', (p['id'],))
        else:
            c.execute('INSERT INTO reservations VALUES (?,?,?,?,1)',
                      (p['id'], p['run_id'], p['quantity'], time.time()))
        return dict(c.execute('SELECT * FROM reservations WHERE id=?', (p['id'],)).fetchone())


def complete(path, order_id):
    with db(path) as c:
        # COALESCE conserva el primer instante; una repetición no lo cambia.
        row = c.execute('UPDATE orders SET status=?, completed=COALESCE(completed,?) '
                        'WHERE id=?', ('CONFIRMADO', time.time(), order_id))
        if row.rowcount != 1:
            raise ValueError('Pedido inexistente')
    return get_order(path, order_id)


def get_order(path, order_id):
    with db(path) as c:
        row = c.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
        return dict(row) if row else None


def report(path, run_id, role, counts=False):
    table = 'orders' if role == 'orders' else 'reservations'
    with db(path) as c:
        if counts and role == 'orders':
            rows = c.execute('SELECT status,COUNT(*) n FROM orders WHERE run_id=? GROUP BY status',
                             (run_id,)).fetchall()
            return {r['status']: r['n'] for r in rows}
        return [dict(r) for r in c.execute(f'SELECT * FROM {table} WHERE run_id=?', (run_id,))]


def request(url, payload=None, timeout=3):
    body = json.dumps(payload).encode() if payload is not None else None
    req = Request(url, body, {'Content-Type': 'application/json'})
    with urlopen(req, timeout=timeout) as response:
        return json.load(response)


def relay_once(path, publish):
    with db(path) as c:
        rows = c.execute('SELECT id,payload FROM outbox WHERE sent IS NULL ORDER BY rowid LIMIT 50').fetchall()
    for row in rows:
        with db(path) as c:
            c.execute('UPDATE outbox SET attempts=attempts+1 WHERE id=?', (row['id'],))
        # publish retorna SOLO después de confirmación del broker.
        publish(row['payload'])
        with db(path) as c:
            c.execute('UPDATE outbox SET sent=? WHERE id=?', (time.time(), row['id']))
    return len(rows)


def consume_one(path, payload, notify):
    reserve(path, payload)
    # El consumidor confirma al broker después de esta notificación.
    # Si falla, la entrega se repite; reserve es idempotente.
    notify(payload['id'])
