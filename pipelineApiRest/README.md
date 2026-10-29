# Pipeline API REST

Pipeline ETL que consume una API REST, transforma órdenes de e-commerce y las guarda en Parquet particionado.

## 1. Descripción del proyecto

Este proyecto implementa el patrón más común en Data Engineering moderno: extraer datos desde una API, transformarlos y persistirlos en un formato columnar optimizado.

La fuente es el dataset `ecommerce` de [Ian Saura](https://iansaura.com/api). El pipeline pide las órdenes, corrige tipos, agrega campos calculados y escribe Parquet particionado por año y mes.

## Objetivos

* Consumir una API REST con `timeout` y manejo de errores HTTP.
* Reintentar fallos transitorios (timeout, 5xx, red) con exponential backoff.
* No reintentar errores 4xx: el problema está en el request, no en el servidor.
* Transformar y validar las órdenes (tipos, nulos, dataset vacío).
* Guardar el resultado en Parquet particionado para lecturas más rápidas.
* Mantener credenciales fuera del código, en variables de entorno.

## 2. Requisitos

* Python 3.11 o superior
* Un token de API generado desde el Dashboard de la plataforma (es privado, no se va a poder ver sin pagar)

## 3. Instalación

```bash
cd pipelineApiRest
pip install -r requirements.txt
```

Dependencias:

* `requests` — llamadas HTTP
* `pandas` — transformación
* `pyarrow` — escritura Parquet
* `python-dotenv` — carga del archivo `.env`

`logging`, `os` y `time` son parte de la biblioteca estándar.

## 4. Configuración


```env
API_TOKEN=tu_token_aqui
API_EMAIL=tu_email_aqui
API_BASE_URL=https://iansaura.com/api
```

`config.py` carga esas variables al inicio. Si falta `API_TOKEN`, el script corta con un error claro.

La API de la plataforma autentica con `email` y `key`. El material del curso usa `token`. El pipeline envía los tres parámetros para cubrir ambos casos. El valor de `API_TOKEN` es la API Key del Dashboard.

## 5. Estructura del proyecto

```text
pipelineApiRest/
├── .env                 # credenciales (no versionado)
├── .gitignore
├── config.py            # carga de variables de entorno
├── main.py              # extract, transform, load
├── requirements.txt
├── README.md
└── output/              # generado al correr (no versionado)
    ├── orders_all.parquet
    └── orders/
        └── order_year=2024/
            └── order_month=2024-01/
                └── data.parquet
```

## 6. Arquitectura ETL

### Extract

`fetch_data()` llama a `{API_BASE_URL}/datasets.php` con `type=ecommerce` y `rows=5000`.

* Usa `timeout=30` para no colgarse si la API no responde.
* `raise_for_status()` convierte respuestas 4xx/5xx en excepciones.

`fetch_data_with_retry()` envuelve esa llamada:

| Error | Comportamiento |
| --- | --- |
| Timeout / red | reintenta hasta 3 veces |
| HTTP 5xx | reintenta (fallo del servidor) |
| HTTP 4xx | no reintenta; corta de inmediato |

Entre intentos espera 2^n segundos (1s, 2s, 4s con `backoff_factor=2`).

### Transform

`transform_data()` toma `tables.orders` del JSON y arma un DataFrame.

* Si no hay filas, loguea un warning y devuelve el DataFrame vacío.
* La API entrega `total_amount`; si no existe `total`, se copia desde `total_amount`.
* Convierte `order_date` a datetime y `total` a numérico (`errors='coerce'`).
* Agrega `order_month`, `order_year`, `is_high_value` (total > 100) y `day_of_week`.
* Loguea cuántas órdenes tienen `total` inválido.

### Load

`save_data()` escribe dos salidas:

1. Particionado: `output/orders/order_year=YYYY/order_month=YYYY-MM/`
2. Consolidado: `output/orders_all.parquet`

Particionar por fecha permite leer solo los meses necesarios en consultas posteriores.

## 7. Cómo ejecutarlo

Desde la carpeta del proyecto:

```bash
python main.py
```

Una corrida correcta termina con el mensaje `Pipeline completado exitosamente!`. En una prueba local se recibieron 5000 órdenes y se generaron 46 particiones mensuales.

## 8. Decisiones de implementación

* Credenciales solo en `.env`. El código no contiene tokens.
* `output/` está ignorado por Git: los datos generados no se versionan.
* Se conserva el campo original `total_amount` y se agrega `total` para alinear el dataset real con el material del curso.
* El pipeline corta si el DataFrame queda vacío después del transform: no escribe Parquet hueco.
