## 1. Qué contiene y qué representa
| Archivo | Función |
|---|---|
| `carga.js` | Genera una solicitud por iteración con k6 y guarda tiempos y códigos HTTP. |
| `core.py` | Define las transacciones locales, la persistencia, la idempotencia y la conciliación básica. |
| `app.py` | Implementa los servicios HTTP de pedidos e inventario. |
| `messaging.py` | Publica y consume eventos con RabbitMQ. |
| `compose.yaml` | Define los servicios, sus redes, almacenamiento y cuotas. |
| `experimento.py` | Inicia el entorno, realiza el calentamiento, aplica la carga y controla el fallo. |
| `analizar.py` | Une registros y produce un CSV y un resumen JSON. |
| `tests/test_instrumento.py` | Verifica propiedades de persistencia, HTTP e interpretación de datos. |
| `VALIDACION.txt` | Distingue verificaciones locales de pruebas pendientes de integración. |

El generador de tráfico es el núcleo de la segunda herramienta. Los prototipos constituyen el objeto de estudio. Los registros y el analizador permiten interpretar los datos.

Esta implementación utiliza Python 3.12 y SQLite para facilitar la revisión del código, y RabbitMQ para la mensajería real. El documento del proyecto plantea Node.js: si se adopta este código, el marco metodológico debe describir Python y SQLite. El cambio no se presenta como una implementación Node.js.

El prototipo directo NO es una Saga completa: no implementa una secuencia de múltiples operaciones con compensaciones. El nombre correcto de esta comparación es «HTTP directo frente a Transactional Outbox en dos prototipos de pedidos». Saga y Outbox pueden coexistir.

## 2. Operación de negocio

Cada petición representa un pedido de una unidad de un producto de laboratorio. Inventario registra una reserva por identificador. El modelo no incluye existencias finitas, pagos ni una tienda visual; evita que el agotamiento de inventario contamine las pruebas de comunicación.

Un resultado verificado exige: pedido confirmado, reserva presente y cantidades iguales. Un HTTP 202 solo significa aceptación. La llave primaria de la tabla de reservas impide dos efectos para un mismo pedido. El contador `deliveries` registra intentos repetidos sin multiplicar la reserva.

### Ruta directa

1. Pedidos persiste el pedido como PENDIENTE.
2. Invoca `/reserve` en inventario por HTTP.
3. Inventario persiste la reserva y responde.
4. Pedidos registra CONFIRMADO y responde al cliente.

Si falla la llamada, responde 503 y conserva PENDIENTE: un timeout no prueba que la reserva no existe. Esta versión no ejecuta reintentos automáticos en la ruta directa. Reenviar manualmente el mismo identificador permite intentar de nuevo sin duplicar efectos, pero el script de carga no hace ese reenvío.

### Ruta Outbox

1. Pedidos guarda pedido y evento dentro de UNA transacción local SQLite.
2. Responde 202 al cliente.
3. El relay lee eventos pendientes y publica con confirmación del broker.
4. Marca el evento como enviado solo después de esa confirmación.
5. Inventario consume, persiste una reserva idempotente y notifica la finalización a pedidos.
6. El consumidor emite ACK solo después de esa notificación.

Si la publicación falla, el evento permanece pendiente. Si el consumidor falla tras reservar, la entrega se puede repetir. La idempotencia conserva una sola reserva. El callback HTTP de finalización forma parte de esta implementación y agrega trabajo: no representa una arquitectura asíncrona universal.

La cola es durable y los mensajes persistentes. Estas propiedades y las confirmaciones reducen ventanas de pérdida, pero no autorizan afirmar garantía absoluta ante cualquier fallo de disco o infraestructura.

## 3. Requisitos y primera ejecución en Windows

Se utiliza Docker Desktop con contenedores Linux, Python 3.12 o compatible y k6 disponibles en PATH. Docker descarga imágenes y dependencias durante la primera construcción. En macOS/Linux se sustituye `py` por `python3`.

Fuentes oficiales de instalación:
- Docker Desktop: https://docs.docker.com/desktop/
- Python: https://www.python.org/downloads/
- k6: https://grafana.com/docs/k6/latest/set-up/install-k6/

Se extrae el ZIP en una carpeta y se abre PowerShell dentro de `MINV_Herramienta_2`.

```powershell
py --version
docker compose version
k6 version
py -m unittest discover -s tests -v
```

Las pruebas locales no requieren instalar Pika: las partes HTTP y SQLite utilizan la biblioteca estándar. Docker instala Pika para los servicios de mensajería.

Prueba corta del prototipo directo:

```powershell
py experimento.py --mode direct --rate 10 --seconds 30 --warmup 10 --followup 15
```

Prueba corta de Outbox:

```powershell
py experimento.py --mode outbox --rate 10 --seconds 30 --warmup 10 --followup 15
```

El orquestador inicia únicamente los servicios del prototipo seleccionado y detiene otros servicios de este mismo proyecto Compose. No ejecuta pruebas simultáneas. Los datos persisten entre ejecuciones y cada ejecución recibe un identificador distinto.

Para una comparación experimental, primero se conservan los archivos de resultados y después se reinician los datos de laboratorio entre ejecuciones independientes. El siguiente comando elimina EXCLUSIVAMENTE los volúmenes de este proyecto Compose, incluidas sus bases y cola; los CSV y JSON en `resultados` permanecen en el equipo:

```powershell
docker compose down --volumes
```

Este reinicio se ejecuta desde la carpeta del proyecto. Sin él, quedan diferencias de tamaño de tablas y pueden quedar mensajes de pruebas anteriores: el identificador de ejecución evita mezclar registros, pero no evita interferencias de rendimiento.

## 4. Prueba de caída del receptor

Prueba corta para verificar el procedimiento, no para producir conclusiones definitivas:

```powershell
py experimento.py --mode direct --rate 10 --seconds 40 --warmup 10 --followup 20 --fault inventory --fault-at 10 --fault-seconds 5
```

Después de conservar resultados y reiniciar el laboratorio, se repite con `--mode outbox` y los demás parámetros iguales. El fallo usa SIGKILL sobre inventario; no pausa todo el equipo. El programa intenta restaurarlo incluso si la prueba presenta una excepción.

La prueba adicional `--fault broker` solo corresponde a Outbox. No se compara directamente con la caída del receptor del prototipo directo.

El momento del fallo se programa a partir de la primera línea de solicitud registrada. Se conservan instantes reales de solicitud de interrupción, interrupción efectiva y restauración. El inicio del contenedor no prueba que todo el procesamiento se estabiliza; la comprobación de salud confirma accesibilidad del proceso.

## 5. Protocolo principal propuesto

Los valores son parámetros del diseño, no resultados ni estándares universales. El piloto confirma si el equipo sostiene la carga y si la instrumentación introduce sobrecarga apreciable.

| Parámetro | Propuesta inicial |
|---|---|
| Cargas | 10, 50 y 100 operaciones/s, como ejecuciones separadas |
| Calentamiento | 300 segundos por ejecución |
| Ventana de carga | 300 segundos |
| Seguimiento | 300 segundos después de terminar k6 |
| Repeticiones | Al menos 3 por condición; se aumenta según la variabilidad |
| Orden | Alternado o aleatorio; se conserva registro del orden |
| Fallo | Receptor fuera de servicio 60 s, a partir de los 120 s |
| Tasa para fallos | Una tasa que ambos prototipos sostienen en el piloto |
| Usuarios virtuales | Capacidad verificada del generador; no un valor deducido como exacto |

Ejemplo de una ejecución normal:

```powershell
py experimento.py --mode direct --rate 10 --seconds 300 --warmup 300 --followup 300 --vus 100
```

Ejemplo con fallo:

```powershell
py experimento.py --mode outbox --rate 10 --seconds 300 --warmup 300 --followup 300 --vus 100 --fault inventory --fault-at 120 --fault-seconds 60
```

Si el calentamiento no resuelve pendientes, el programa detiene la prueba. Un escenario de saturación requiere un calentamiento de menor tasa y un diseño específico: la versión actual utiliza la misma tasa de calentamiento y medición. No se oculta esta restricción ni se presenta como un ensayo de capacidad máxima completo.

Los servicios compartidos disponen de las mismas cuotas (1 CPU, 256 MiB) y un trabajador efectivo de inventario. Un retardo controlado de 10 ms representa procesamiento idéntico; se documenta como demora artificial. El broker añade 1 CPU y 512 MiB, por lo que NO se iguala el presupuesto total de recursos. La ejecución no garantiza una tasa real determinada: k6 informa iteraciones omitidas si no dispone de capacidad.

## 6. Resultados y lectura

Cada ejecución crea `resultados/<run_id>/`:

- `metadata.json`: configuración, versiones, hashes del código, instantes de fallo y errores.
- `compose_resuelto.yaml` e `imagenes.txt`: infraestructura y referencias de imágenes.
- `requests.log`: observaciones por solicitud en formato JSON con prefijo TX.
- `k6_summary.json`: métricas del generador, incluidas iteraciones omitidas.
- `orders.json` y `reservations.json`: evidencia del estado de los servicios.
- `counts_followup.json`: conteos durante el seguimiento.
- `transacciones.csv`: unión de solicitudes y resultados, accesible desde Excel.
- `resumen.json`: indicadores por ejecución.
- `warmup/`: evidencia separada del calentamiento; no se mezcla con la cohorte principal.

Se puede repetir el análisis sin repetir la carga:

```powershell
py analizar.py resultados/NOMBRE_REAL_DE_LA_EJECUCION
```

### Definiciones operativas

- Latencia de respuesta: fin menos inicio de la solicitud, medidos por el generador; incluye la espera del cliente.
- Latencia de finalización: confirmación persistente del pedido menos inicio del cliente. Requiere relojes compatibles entre host y contenedores. Los valores negativos se señalan como incompatibilidad.
- Percentiles: método de rango más próximo. Los percentiles finales incluyen SOLO las operaciones confirmadas; pendientes y no observadas se muestran junto a ellos.
- Éxito: pedidos confirmados con reserva coherente / solicitudes iniciadas con registro. Si faltan líneas respecto al número de iteraciones de k6, el denominador es incompleto y la ejecución no resulta apta para concluir.
- Throughput: finalizaciones verificadas que caen dentro de la ventana de carga / duración de esa ventana. Las finalizaciones posteriores sí cuentan en el éxito de la cohorte, pero no en ese throughput.
- PENDIENTE: existe un pedido sin confirmación completa al cierre. No equivale automáticamente a pérdida ni a fallo permanente.
- NO_OBSERVADO: el registro del servicio no permite ubicar la solicitud; no prueba que el mensaje se pierde.
- INCONSISTENTE: el pedido declara confirmación pero no aparece una reserva coherente.
- Entregas repetidas: intentos adicionales sobre una reserva existente; no son efectos duplicados.
- Resolución de cohorte anterior a restauración: demora hasta la última finalización de todas las operaciones anteriores a la restauración. Si alguna sigue pendiente, se informa null.

Esta última métrica NO es el RTO ni el tiempo de recuperación sostenida del throughput. El RTO es un objetivo. Esta versión deja `recuperacion_operativa_s` en null; requiere series temporales durante carga y un criterio previo para estimarlo. No atribuye automáticamente cero segundos a datos ausentes.

## 7. Calidad, límites y coherencia con el documento

1. La comparación incluye políticas diferentes: sin reintento automático en HTTP directo y con persistencia/reintentos en Outbox. Las diferencias de confiabilidad se atribuyen al conjunto de esas decisiones, no exclusivamente al protocolo.
2. SQLite y el servidor HTTP estándar son componentes de laboratorio. Sus bloqueos y límites pueden dominar la latencia. No representan PostgreSQL ni un servidor Node.js de producción.
3. El registro por solicitud consume recursos. Se verifica en el piloto que el generador y la escritura de logs no limitan la prueba. No se presupone capacidad para cientos de miles de operaciones por segundo.
4. No se incluye Prometheus/Grafana ni CPU/memoria observadas: las cuotas de Compose no equivalen a mediciones. Esto corresponde a la tercera herramienta.
5. El consumidor tiene un trabajador y confirmación HTTP; el costo de esta elección se documenta.
6. Las imágenes utilizan etiquetas de versión, no digests inmutables. Antes de la campaña formal se fijan digests y se registran CPU, RAM, sistema operativo, versión de Docker y límites reales del equipo.
7. Los endpoints de laboratorio no tienen autenticación. Los puertos se vinculan a localhost y las credenciales son locales de demostración. No es un despliegue público.
8. No se adopta «100 % esperado» como criterio de resultados. El análisis informa lo que muestran los registros.
9. Las repeticiones independientes sustentan la variabilidad entre ejecuciones. Muchas solicitudes dentro de una sola ejecución no garantizan significancia estadística.
10. La pestaña metodológica debe distinguir conexión directa de Saga y tiempo observado de recuperación de RTO. El cronograma debe referirse a software/transacciones, no a encuestas a estudiantes.
11. Un fallo de volumen, disco, mensaje malformado o apagado del host queda fuera del escenario principal. Los mensajes del experimento tienen un formato controlado; la gestión de mensajes inválidos necesita una cola de descarte antes de ampliar el dominio.
12. El script conserva el trabajo previo del laboratorio. Se reinicia su almacenamiento de forma explícita entre condiciones; no basta con cambiar el run_id.

## 8. Qué verifica la prueba local

`py -m unittest discover -s tests -v` comprueba atomicidad con rollback inducido, reintento después de error de publicación simulado, idempotencia concurrente, fallo de notificación, conflictos de identificadores, estabilidad de finalización, HTTP real entre procesos lógicos en hilos, rechazo de entradas inválidas y separación entre aceptación/éxito.

Los dobles de publicación permiten probar lógica de error, pero no sustituyen RabbitMQ real. La integración Docker + RabbitMQ + k6 requiere ejecutar las pruebas cortas anteriores en el equipo de aplicación. No existe un benchmark final incluido en este paquete.

## 9. Referencias técnicas

- Richardson, C. Transactional outbox. https://microservices.io/patterns/data/transactional-outbox.html
- Grafana. Constant arrival rate. https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/
- Grafana. Arrival-rate VU allocation. https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/arrival-rate-vu-allocation/
- RabbitMQ. Work Queues (Python). https://www.rabbitmq.com/tutorials/tutorial-two-python
- Pika. Mandatory publishing. https://pika.readthedocs.io/en/stable/examples/blocking_publish_mandatory.html

Estas fuentes orientan la implementación. Los valores de carga y duración son decisiones del protocolo que se validan mediante piloto.
