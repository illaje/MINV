"""Orquesta una ejecución local de laboratorio y conserva su evidencia."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
import uuid
from urllib.parse import quote
from core import request
from analizar import analyze

ROOT = Path(__file__).resolve().parent


def compose(*args):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True,
                          capture_output=True, text=True, timeout=180)


def wait_health(url):
    deadline = time.monotonic()+90
    while time.monotonic() < deadline:
        try:
            request(url+'/health', timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError('El servicio no alcanza un estado accesible: '+url)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['direct', 'outbox'], required=True)
    p.add_argument('--rate', type=int, default=10)
    p.add_argument('--seconds', type=int, default=30)
    p.add_argument('--vus', type=int, default=100)
    p.add_argument('--warmup', type=int, default=30)
    p.add_argument('--followup', type=int, default=60)
    p.add_argument('--fault', choices=['none', 'inventory', 'broker'], default='none')
    p.add_argument('--fault-at', type=int, default=10)
    p.add_argument('--fault-seconds', type=int, default=5)
    a = p.parse_args()
    if min(a.rate, a.seconds, a.vus, a.followup, a.fault_seconds) <= 0 or a.warmup < 0:
        p.error('Los parámetros requieren valores positivos; warmup admite cero')
    if a.fault == 'broker' and a.mode != 'outbox':
        p.error('La prueba de broker corresponde exclusivamente a outbox')
    if a.fault != 'none' and not 0 <= a.fault_at < a.fault_at+a.fault_seconds < a.seconds:
        p.error('El fallo y su restauración deben quedar dentro de la ventana de carga')
    for binary in ('docker', 'k6'):
        if not shutil.which(binary):
            p.error(f'Falta {binary} en PATH. Consulta README.md')
    run_id = a.mode+'-'+uuid.uuid4().hex[:12]
    folder = ROOT/'resultados'/run_id
    folder.mkdir(parents=True)
    url = 'http://127.0.0.1:'+('8001' if a.mode == 'direct' else '8002')
    inv_url = 'http://127.0.0.1:'+('8101' if a.mode == 'direct' else '8102')
    meta = {**vars(a), 'run_id': run_id, 'errors': [],
            'codigo_sha256': {f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                             for f in ROOT.iterdir() if f.suffix in ('.py', '.js', '.yaml')},
            'k6_version': subprocess.check_output(['k6', 'version'], text=True).strip(),
            'aviso': 'Prueba de laboratorio; no representa rendimiento de producción.'}
    def save_meta():
        (folder/'metadata.json').write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding='utf-8')
    # Stop conserva volúmenes. No borra pruebas previas ni datos automáticamente.
    compose('stop')
    services = ['orders-direct', 'inventory-direct'] if a.mode == 'direct' else ['orders-outbox', 'inventory-outbox', 'rabbit']
    compose('up', '-d', '--build', *services)
    wait_health(url)
    wait_health(inv_url)
    (folder/'compose_resuelto.yaml').write_text(compose('config').stdout, encoding='utf-8')
    (folder/'imagenes.txt').write_text(compose('images').stdout, encoding='utf-8')
    def load(run, seconds, output):
        output.mkdir(exist_ok=True)
        env = {**os.environ, 'BASE_URL': url, 'RUN_ID': run, 'RATE': str(a.rate),
               'SECONDS': str(seconds), 'VUS': str(a.vus),
               'SUMMARY_PATH': str(output/'k6_summary.json')}
        with (output/'k6_proceso.log').open('w', encoding='utf-8') as log:
            return subprocess.run(['k6', 'run', '--log-format', 'raw', '--console-output',
                str(output/'requests.log'), str(ROOT/'carga.js')], env=env, cwd=ROOT,
                stdout=log, stderr=subprocess.STDOUT, timeout=seconds+45).returncode
    if a.warmup:
        warm_id = run_id+'-warmup'
        if load(warm_id, a.warmup, folder/'warmup') != 0:
            raise RuntimeError('El calentamiento presenta errores. Revisa su log.')
        deadline = time.monotonic()+a.followup
        while request(url+'/counts?run_id='+warm_id).get('PENDIENTE', 0):
            if time.monotonic() > deadline:
                raise RuntimeError('El calentamiento no resuelve sus pendientes; reduce la tasa.')
            time.sleep(1)
    cancelled = threading.Event()
    def fault():
        if a.fault == 'none':
            return
        # Sincroniza el fallo con la primera solicitud registrada, no con el arranque de k6.
        first_request = folder/'requests.log'
        while not cancelled.is_set():
            if first_request.exists() and 'TX ' in first_request.read_text(encoding='utf-8'):
                break
            cancelled.wait(0.05)
        if cancelled.is_set() or cancelled.wait(a.fault_at):
            return
        service = 'rabbit' if a.fault == 'broker' else 'inventory-'+a.mode
        try:
            meta['fault_requested_time'] = time.time()
            compose('kill', '-s', 'SIGKILL', service)
            meta['fault_effective_time'] = time.time()
            cancelled.wait(a.fault_seconds)
        except Exception as error:
            meta['errors'].append('Interrupción: '+str(error))
        finally:
            try:
                compose('start', service)
                if service != 'rabbit':
                    wait_health(inv_url)
                else:
                    deadline = time.monotonic()+90
                    while True:
                        try:
                            compose('exec', '-T', 'rabbit', 'rabbitmq-diagnostics', '-q', 'ping')
                            break
                        except subprocess.CalledProcessError:
                            if time.monotonic() > deadline:
                                raise RuntimeError('RabbitMQ no responde')
                            time.sleep(1)
                meta['fault_restored_time'] = time.time()
            except Exception as error:
                meta['errors'].append('Restauración: '+str(error))
    thread = threading.Thread(target=fault)
    try:
        meta['launch_time'] = time.time()
        thread.start()
        meta['k6_exit_code'] = load(run_id, a.seconds, folder)
    finally:
        cancelled.set()
        thread.join()
        save_meta()
    # Ventana explícita después de terminar el proceso k6 (incluye su gracefulStop).
    meta['followup_start'] = time.time()
    samples = []
    deadline = time.monotonic()+a.followup
    while time.monotonic() < deadline:
        samples.append({'time': time.time(), **request(url+'/counts?run_id='+quote(run_id))})
        time.sleep(1)
    meta['observation_closed'] = time.time()
    (folder/'counts_followup.json').write_text(json.dumps(samples), encoding='utf-8')
    for filename, target in [('orders.json', url), ('reservations.json', inv_url)]:
        (folder/filename).write_text(json.dumps(request(target+'/records?run_id='+quote(run_id))), encoding='utf-8')
    # Congela el corte temporal aunque una escritura termina durante la exportación.
    orders = json.loads((folder/'orders.json').read_text())
    for o in orders:
        if o['completed'] and o['completed'] > meta['observation_closed']:
            o['completed'], o['status'] = None, 'PENDIENTE'
    (folder/'orders.json').write_text(json.dumps(orders), encoding='utf-8')
    save_meta()
    analyze(folder)
    print('Archivos de la ejecución:', folder)


if __name__ == '__main__':
    main()
