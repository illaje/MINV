"""Une solicitudes y estados, exporta CSV y calcula métricas sin librerías extra."""
import argparse
import csv
import json
import math
from pathlib import Path


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]


def read_requests(log):
    records = []
    for line in log.splitlines():
        if line.startswith('TX '):
            records.append(json.loads(line[3:]))
    return records


def summarize(requests, orders, reservations, start, end):
    """Cohorte: solicitudes iniciadas. Ventana throughput: [start,end)."""
    if end <= start:
        raise ValueError('La ventana debe tener duración positiva')
    by_id = {o['id']: o for o in orders}
    inventory = {r['id']: r for r in reservations}
    if len({r['id'] for r in requests}) != len(requests):
        raise ValueError('El log contiene identificadores de solicitud repetidos')
    joined = []
    for r in requests:
        o, reservation = by_id.get(r['id']), inventory.get(r['id'])
        ok = bool(o and o['status'] == 'CONFIRMADO' and reservation
                  and o['quantity'] == reservation['quantity'])
        if ok:
            state = 'CONFIRMADO'
        elif o and o['status'] == 'CONFIRMADO':
            state = 'INCONSISTENTE'
        elif o:
            state = 'PENDIENTE'
        else:
            state = 'NO_OBSERVADO'
        completion = o['completed'] if ok else None
        joined.append({**r, 'estado_verificado': state,
            'completed': completion,
            'completion_ms': (completion-r['start'])*1000 if completion is not None else None,
            'reservation_exists': bool(reservation),
            'deliveries': reservation['deliveries'] if reservation else 0})
    good = [r for r in joined if r['estado_verificado'] == 'CONFIRMADO']
    latencies = [r['completion_ms'] for r in good]
    result = {
        'iniciadas_con_registro': len(joined),
        'completadas_verificadas': len(good),
        'exito_pct': 100 * len(good) / len(joined) if joined else None,
        'errores_http_o_transporte': sum(r['http_status'] not in (200, 202) for r in joined),
        'pendientes': sum(r['estado_verificado'] == 'PENDIENTE' for r in joined),
        'no_observadas': sum(r['estado_verificado'] == 'NO_OBSERVADO' for r in joined),
        'inconsistentes': sum(r['estado_verificado'] == 'INCONSISTENTE' for r in joined),
        'entregas_repetidas': sum(max(0, r['deliveries']-1) for r in joined),
        'throughput_finalizaciones_ventana_s': sum(start <= r['completed'] < end for r in good)/(end-start),
        'ventana_inicio': start, 'ventana_fin': end,
        'advertencia': 'Percentiles de finalización solo incluyen operaciones confirmadas; se interpretan junto con pendientes.',
        'relojes_incompatibles': any(x < 0 for x in latencies),
    }
    for p in (50, 95, 99):
        result[f'respuesta_p{p}_ms'] = percentile([r['response_ms'] for r in joined], p)
        result[f'finalizacion_p{p}_ms'] = percentile(latencies, p)
    return result, joined


def analyze(folder):
    folder = Path(folder)
    meta = json.loads((folder/'metadata.json').read_text())
    reqs = read_requests((folder/'requests.log').read_text(encoding='utf-8'))
    orders = json.loads((folder/'orders.json').read_text())
    inventory = json.loads((folder/'reservations.json').read_text())
    # La primera solicitud delimita el comienzo efectivo, no el arranque del proceso k6.
    start = min((r['start'] for r in reqs), default=meta['launch_time'])
    summary, rows = summarize(reqs, orders, inventory, start, start+meta['seconds'])
    k6 = json.loads((folder/'k6_summary.json').read_text())
    metrics = k6.get('metrics', {})
    iterations = metrics.get('iterations', {}).get('values', {}).get('count', 0)
    dropped = metrics.get('dropped_iterations', {}).get('values', {}).get('count', 0)
    summary.update({'k6_iterations': iterations, 'k6_dropped_iterations': dropped,
        'log_completo': iterations > 0 and iterations == len(reqs), 'run_id': meta['run_id'],
        'mode': meta['mode'], 'carga_objetivo_s': meta['rate'],
        'carga_iniciada_con_registro_s': len(reqs)/meta['seconds'],
        'limitacion_generador': dropped > 0,
        'incidencias_orquestador': meta.get('errors', []),
        'validacion_pendiente': 'Estos datos no garantizan validez externa ni significancia estadística.'})
    if meta.get('fault_restored_time'):
        restored = meta['fault_restored_time']
        affected = [r for r in rows if r['start'] < restored]
        all_complete = bool(affected) and all(r['completed'] is not None for r in affected)
        summary['resolucion_cohorte_pre_restauracion_s'] = (
            max(0, max(r['completed'] for r in affected)-restored) if all_complete else None)
        summary['recuperacion_operativa_s'] = None
        summary['nota_recuperacion'] = ('La resolución de la cohorte no equivale a recuperación sostenida '
                                        'del throughput; esta versión no estima esa recuperación.')
    (folder/'resumen.json').write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')
    if rows:
        with (folder/'transacciones.csv').open('w', newline='', encoding='utf-8-sig') as out:
            writer = csv.DictWriter(out, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('folder')
    analyze(parser.parse_args().folder)
