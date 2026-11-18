import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
import core
from app import make_server
from analizar import summarize, percentile, read_requests

class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name)/'orders.sqlite')
        self.inv = str(Path(self.temp.name)/'inventory.sqlite')
        core.init(self.path)
        core.init(self.inv)
        self.p = {'id': 'run-1', 'run_id': 'run', 'quantity': 2}
    def tearDown(self):
        self.temp.cleanup()
    def test_order_and_event_rollback_together(self):
        with core.db(self.path) as c:
            c.execute("CREATE TRIGGER reject_event BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'failure'); END")
        with self.assertRaises(Exception):
            core.create_order(self.path, self.p, 'outbox')
        self.assertIsNone(core.get_order(self.path, self.p['id']))
    def test_failed_publication_stays_pending_and_retries(self):
        core.create_order(self.path, self.p, 'outbox')
        with self.assertRaises(RuntimeError):
            core.relay_once(self.path, lambda _: (_ for _ in ()).throw(RuntimeError('broker down')))
        with core.db(self.path) as c:
            row = c.execute('SELECT * FROM outbox').fetchone()
            self.assertIsNone(row['sent'])
            self.assertEqual(row['attempts'], 1)
        sent = []
        core.relay_once(self.path, sent.append)
        self.assertEqual(len(sent), 1)
        self.assertEqual(core.relay_once(self.path, sent.append), 0)
    def test_duplicate_delivery_has_one_effect(self):
        core.reserve(self.inv, self.p)
        core.reserve(self.inv, self.p)
        rows = core.report(self.inv, 'run', 'inventory')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['quantity'], 2)
        self.assertEqual(rows[0]['deliveries'], 2)
    def test_callback_failure_replays_without_duplicate_reservation(self):
        core.create_order(self.path, self.p, 'outbox')
        with self.assertRaises(RuntimeError):
            core.consume_one(self.inv, self.p, lambda _: (_ for _ in ()).throw(RuntimeError('offline')))
        core.consume_one(self.inv, self.p, lambda id: core.complete(self.path, id))
        self.assertEqual(core.get_order(self.path, self.p['id'])['status'], 'CONFIRMADO')
        self.assertEqual(len(core.report(self.inv, 'run', 'inventory')), 1)
    def test_id_payload_conflict_rejected(self):
        core.create_order(self.path, self.p, 'outbox')
        with self.assertRaises(ValueError):
            core.create_order(self.path, {**self.p, 'quantity': 3}, 'outbox')
    def test_completion_timestamp_is_stable(self):
        core.create_order(self.path, self.p, 'outbox')
        first = core.complete(self.path, self.p['id'])
        second = core.complete(self.path, self.p['id'])
        self.assertEqual(first['completed'], second['completed'])
    def test_concurrent_duplicates_have_one_effect(self):
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: core.reserve(self.inv, self.p), range(20)))
        rows = core.report(self.inv, 'run', 'inventory')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['deliveries'], 20)

class HTTPTests(unittest.TestCase):
    def test_direct_http_and_async_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            inv = make_server('inventory', 'direct', str(Path(tmp)/'inv.db'), 0, host='127.0.0.1')
            inv_url = 'http://127.0.0.1:'+str(inv.server_port)
            direct = make_server('orders', 'direct', str(Path(tmp)/'direct.db'), 0, inv_url, '127.0.0.1')
            asynchronous = make_server('orders', 'outbox', str(Path(tmp)/'async.db'), 0, host='127.0.0.1')
            servers = [inv, direct, asynchronous]
            threads = [threading.Thread(target=s.serve_forever) for s in servers]
            for t in threads: t.start()
            try:
                p = {'id': 'direct-1', 'run_id': 'run', 'quantity': 1}
                direct_url = 'http://127.0.0.1:'+str(direct.server_port)
                async_url = 'http://127.0.0.1:'+str(asynchronous.server_port)
                self.assertEqual(core.request(direct_url+'/orders', p)['status'], 'CONFIRMADO')
                self.assertEqual(core.request(async_url+'/orders', {**p, 'id': 'outbox-1'})['status'], 'PENDIENTE')
                with self.assertRaises(HTTPError) as error:
                    core.request(direct_url+'/orders', {**p, 'quantity': 0})
                self.assertEqual(error.exception.code, 400)
            finally:
                for s in servers:
                    s.shutdown()
                    s.server_close()
                for t in threads: t.join()
    def test_downstream_failure_never_confirms_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            server = make_server('orders', 'direct', str(Path(tmp)/'orders.db'), 0,
                                 'http://127.0.0.1:1', '127.0.0.1')
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                with self.assertRaises(HTTPError) as error:
                    core.request('http://127.0.0.1:'+str(server.server_port)+'/orders',
                                 {'id': 'a', 'run_id': 'r', 'quantity': 1})
                self.assertEqual(error.exception.code, 503)
                self.assertEqual(core.get_order(str(Path(tmp)/'orders.db'), 'a')['status'], 'PENDIENTE')
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

class AnalysisTests(unittest.TestCase):
    def test_acceptance_not_success_and_incomplete_in_denominator(self):
        reqs = [{'id': str(i), 'start': 100, 'end': 100.01,
                 'response_ms': 10, 'http_status': 202} for i in range(2)]
        orders = [{'id': '0', 'status': 'CONFIRMADO', 'quantity': 1, 'completed': 101},
                  {'id': '1', 'status': 'PENDIENTE', 'quantity': 1, 'completed': None}]
        inv = [{'id': '0', 'quantity': 1, 'deliveries': 1}]
        result, rows = summarize(reqs, orders, inv, 100, 110)
        self.assertEqual(result['exito_pct'], 50)
        self.assertEqual(result['pendientes'], 1)
        self.assertEqual(result['finalizacion_p95_ms'], 1000)
        self.assertEqual(result['throughput_finalizaciones_ventana_s'], 0.1)
    def test_confirmation_without_reservation_is_inconsistent(self):
        reqs = [{'id': 'a', 'start': 0, 'end': 1, 'response_ms': 1000, 'http_status': 200}]
        orders = [{'id': 'a', 'status': 'CONFIRMADO', 'quantity': 1, 'completed': 1}]
        result, _ = summarize(reqs, orders, [], 0, 2)
        self.assertEqual(result['inconsistentes'], 1)
        self.assertEqual(result['exito_pct'], 0)
    def test_nearest_rank_and_raw_log(self):
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertIsNone(percentile([], 95))
        self.assertEqual(read_requests('noise\nTX {"id":"a"}\n'), [{'id': 'a'}])

if __name__ == '__main__':
    unittest.main()
