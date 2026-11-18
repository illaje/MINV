// Cada iteración crea UNA operación; la aceptación no prueba su finalización.
import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';

const rate = Number(__ENV.RATE || 10);
const duration = Number(__ENV.SECONDS || 30);
const vus = Number(__ENV.VUS || 100);
const base = __ENV.BASE_URL || 'http://127.0.0.1:8001';
const run = __ENV.RUN_ID;
if (!run) throw new Error('RUN_ID es obligatorio');
if (![rate, duration, vus].every(x => Number.isInteger(x) && x > 0)) {
  throw new Error('RATE, SECONDS y VUS deben ser enteros positivos');
}
export const options = {
  scenarios: {
    arrivals: {
      executor: 'constant-arrival-rate', rate, timeUnit: '1s',
      duration: `${duration}s`, preAllocatedVUs: vus, maxVUs: vus,
      gracefulStop: '10s',
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  // Un fallo HTTP durante la inyección es un dato, no una causa de aborto.
  thresholds: { dropped_iterations: ['count==0'] },
};

export default function () {
  const id = `${run}-${exec.scenario.iterationInTest}`;
  const start = Date.now() / 1000;
  const response = http.post(`${base}/orders`, JSON.stringify({
    id, run_id: run, quantity: 1,
  }), { headers: {'Content-Type': 'application/json'}, timeout: '5s',
       tags: {name: 'crear_pedido'} });
  const end = Date.now() / 1000;
  check(response, { 'HTTP acepta o completa': r => r.status === 200 || r.status === 202 });
  // El orquestador guarda estas líneas en un archivo; no son datos inventados.
  console.log('TX ' + JSON.stringify({id, run_id: run, start, end,
    response_ms: (end-start)*1000, http_status: response.status,
    error_code: response.error_code || 0}));
}

export function handleSummary(data) {
  return { [__ENV.SUMMARY_PATH || 'k6_summary.json']: JSON.stringify(data, null, 2) };
}
