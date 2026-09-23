# Entrega 2 - Analisis exploratorio

**Fecha de entrega: miercoles 23 de septiembre.** 20 minutos por grupo, en
privado con el docente. La evaluacion es **individual**: las preguntas van
dirigidas a integrantes puntuales, y la pregunta sobre una hipotesis le
puede caer a quien hizo la limpieza.

Este archivo es el plan de trabajo, no el entregable. El entregable es el
notebook ejecutado mas las fichas y la tabla de columnas candidatas.

---

## 0. El dataset con el que se presenta

`resultados/20260922/brujula_inmobiliaria_alquiler_mendoza.csv`

| Que | Entrega 1 (20260908) | **Entrega 2 (20260922)** |
|---|---|---|
| Filas x columnas | 1673 x 39 | **2504 x 47** |
| Clave `clave` duplicada | 0 | **0** |
| Filas con lat/lon | 1559 (93,2 %) | **2345 (93,7 %)** |
| Avisos en ARS / USD | 1465 / 140 | **2274 / 221** |
| Columnas constantes | 3 | **2** (`condicion`, `provincia`) |
| Features geograficas | 0 | **4** |
| Fechas de scraping | 1 | **4** (07, 08, 09 y 22 de septiembre) |

**831 filas mas que en la Entrega 1**, de las cuales 778 son avisos vistos
por primera vez el 22. Las ocho columnas nuevas son las cuatro geograficas
(`tiene_geo`, `dist_centro_km`, `zona_geo`, `densidad_1km`) y las cuatro del
historial (`primera_vista`, `ultima_vista`, `veces_visto`, `dias_publicado`).

### Como se llego ahi: el dataset acumula fechas

Se corrio el pipeline de nuevo el 22/09 y, ademas, se cambio `parse_raw`
para que lea **todas** las fechas de `data/raw/` en vez de una sola
(el detalle esta en la seccion 2 bis).

La razon del cambio: cada scrapeo es una **foto** del inventario publicado
ese dia, no un incremento. Leyendo una sola fecha, los avisos que se dieron
de baja no existen para el dataset aunque su HTML este en disco. Medido
entre corridas consecutivas aparecen entre **17 y 35 avisos nuevos por dia**,
asi que la union suma filas reales.

> Si una corrida se corta, se retoma con el parametro `fecha` en la fecha de
> esa corrida: lo que ya esta en disco no se vuelve a pedir.

---

## 1. Correccion de Entrega 1: scheduling

El docente marco que falta. Hoy el DAG es `schedule=None` (manual).

**Que cambiar,** en `dags/brujula_pipeline.py`:

```python
@dag(
    dag_id="brujula_inmobiliaria_pipeline",
    schedule="0 9 * * *",        # 06:00 en Argentina; el contenedor corre en UTC
    start_date=datetime(2026, 9, 1),
    catchup=False,               # no rellenar hacia atras: los avisos de ayer ya no estan publicados
    max_active_runs=1,           # dos scrapeos simultaneos sobre el mismo portal no
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["brujula-inmobiliaria", "entrega-2"],
    params={
        "max_paginas": None,
        "fuentes": None,
        "fecha": None,
        "acumular_historico": True,
    },
)
```

**Ya esta aplicado** en `dags/brujula_pipeline.py`.

**Las tres decisiones que hay que poder defender,** porque son la razon de
ser de la correccion y es probable que las pregunten:

1. **Por que diario y no por hora.** El precio de un alquiler no se mueve en
   horas, y cada corrida son ~2.400 pedidos a los portales. Diario es el
   intervalo mas corto que aporta informacion nueva sin castigar al servidor
   ajeno.
2. **Por que `catchup=False`.** Un backfill al 1 de septiembre no traeria los
   avisos de esa fecha: los portales solo muestran lo que esta publicado
   *hoy*. Correr hacia atras scrapearia 22 veces el mismo listado actual y lo
   guardaria bajo 22 fechas distintas, que es peor que no tener el dato.
   Esta es la respuesta que separa "puse un cron" de "entendi que el dato es
   una foto".
3. **Por que `max_active_runs=1`.** Una corrida tarda ~65 min. Sin este
   limite, si una se atrasa arranca la siguiente encima y duplican pedidos.

**Una cuarta decision, que salio al revisarla.** La primera version de este
plan proponia que `extract_listings` tomara la fecha de
`context["data_interval_start"]` en vez de `date.today()`, que es lo que se
hace habitualmente en Airflow. **Se descarto, y el motivo vale como
respuesta si preguntan por que no esta.**

Airflow cierra el intervalo al final: la corrida que se ejecuta el 23 a las
09:00 tiene `data_interval_start` en el **22** a las 09:00. La capa bronce
quedaria en `data/raw/20260922` con avisos scrapeados el 23, que para este
pipeline es directamente la fecha equivocada: la foto es del dia en que se
pidio el HTML, no del intervalo que representa.

En un pipeline que procesa datos historicos (los logs de ayer, las ventas
del mes pasado) el `data_interval` es lo correcto. En uno que scrapea el
estado *actual* de un sitio, no: ahi la fecha es el reloj. Se dejo
`date.today()`. Si en algun momento se quiere la fecha del intervalo, la que
sirve es `data_interval_end`, no `start`.

**Efecto lateral, ya verificado:** `fecha_scraping` era constante cuando el
dataset era una sola corrida, y por eso figuraba entre las columnas a
descartar. Con la acumulacion de fechas toma 4 valores y pasa a ser una
columna con informacion temporal real.

---

## 2. Correccion de Entrega 1: features con latitud y longitud

La otra marca del docente. En la Entrega 1 `latitud` y `longitud` estaban en
el CSV pero crudas: nadie las usaba. Son **2345 filas con coordenadas
(93,7 %)** en el dataset final, concentradas en el Gran Mendoza.

**Antes de derivar nada, limpiar.** Hay 2 filas con coordenadas imposibles:
una en Salta (-24,73) y una en Peru (-9,19 / -75,02), las dos con
`localidad = Capital`. Es el portal que guardo mal el punto en el mapa.

```python
fuera = ~(df.latitud.between(-35.8, -31.8) & df.longitud.between(-70.0, -66.0))
df.loc[fuera, ["latitud", "longitud"]] = np.nan
```

`np.nan` y no `pd.NA`: las dos columnas son `float64`, y `pd.NA` las
convierte a `object`, que despues rompe el haversine.

Esto ademas explica el `skew` de 33,5 en `latitud` del perfil: con esas dos
filas adentro, la asimetria es un artefacto, no una propiedad del dato.

**Las features a construir** (van en `transform.py`, no en el notebook, para
que queden en el pipeline):

| Feature | Como se calcula | Por que aporta |
|---|---|---|
| `dist_centro_km` | Haversine contra Plaza Independencia (-32.8908, -68.8458) | La distancia al centro es el driver clasico del precio por m2; convierte dos numeros sin sentido propio en uno con interpretacion directa |
| `zona_geo` | `KMeans(n_clusters=6)` sobre el aglomerado; el interior queda como `-1` | Agrupa barrios reales, mas fino que `localidad` (que solo tiene 11 valores y mete todo Capital en una bolsa de 627 filas) |
| `densidad_1km` | Vecinos dentro de 1 km, con `BallTree` y metrica haversine | Distingue corredor de departamentos de zona residencial dispersa |
| `tiene_geo` | `latitud.notna()` | El 6,3 % sin coordenadas no es aleatorio: hay que poder marcarlo en vez de imputar |

El radio quedo en 1 km y no en los 500 m que decia la primera version de
este plan: con 500 m, en las zonas menos densas la cuenta da cero para
demasiados avisos y la columna deja de discriminar.

Haversine, para tenerlo a mano:

```python
import numpy as np

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp, dl = p2 - p1, np.radians(lon2 - lon1)
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))

CENTRO_LAT, CENTRO_LON = -32.8908, -68.8458
df["dist_centro_km"] = haversine_km(df.latitud, df.longitud, CENTRO_LAT, CENTRO_LON)
```

**Lo que aparecio al construir `zona_geo`, y que conviene contar.** La
primera version corria KMeans sobre todas las coordenadas y daba clusters
inservibles: uno con 1353 avisos y otro con 2. La causa es que el dataset
llega hasta 255 km del centro (hay avisos en San Martin, Tunuyan, General
Alvear), y KMeans gastaba grupos en esos pocos puntos lejanos mientras metia
todo el Gran Mendoza en un solo cluster, que es justo lo contrario de lo que
la feature tiene que hacer.

La correccion es clusterizar solo el aglomerado (30 km) y dejar el interior
como categoria propia (`zona_geo = -1`). En el dataset final el reparto es
**463 / 138 / 274 / 983 / 146 / 311**, mas 30 en el interior y 159 sin
coordenadas: ningun grupo degenerado.

Es un buen ejemplo para los 2 minutos de "que cambio": la feature existia,
corria sin error y estaba mal. Lo que la delato fue mirar el `value_counts`,
no el codigo.

> **Cuidado con una feature tentadora:** el `precio_m2` promedio del
> vecindario parece la mejor variable geografica posible, y lo es, pero
> calculada sobre todo el dataset es fuga: usa el objetivo de las filas
> vecinas, incluidas las de validacion. Si se arma, se arma solo con el set
> de entrenamiento, y eso es tema de Entrega 3. Para esta entrega alcanza con
> nombrarla y decir por que no esta. Mencionarlo suma; meterla sin aclarar,
> resta.

---

## 2 bis. El dataset acumula fechas

No lo pidio la catedra: salio de preguntarse si re-scrapear daba mas datos.
La respuesta corta es que **no**, y entenderlo cambio el diseño.

**Un scrapeo no suma filas, las reemplaza.** Cada corrida baja el inventario
publicado *ese dia*. Las corridas del 07 y del 08 tienen practicamente los
mismos avisos, porque son el mismo inventario mirado con un dia de
diferencia. Re-scrapear hoy da otra vez ~1.700 filas, no 3.400.

**Lo que si suma es unir las fechas.** Medido con los archivos en disco:

| Comparacion | Avisos nuevos |
|---|---|
| 07 -> 08 (un dia) | 17 |
| 08 -> 09 (un dia) | 35 de 721 revisados (4,9 %) |

Entre el 08 y el 22 pasaron 14 dias, y el listado de Inmoclick paso de 706 a
**835 avisos**. Esos avisos nuevos son filas reales que no estaban.

**Como se implemento.** `parse_raw` lee todas las fechas de `data/raw/` y le
pone a cada registro su `fecha_scraping`. `transform_clean` deduplica por
`clave` quedandose con la observacion **mas reciente** (`keep="last"` sobre
las fechas ordenadas), porque el precio publicado pudo cambiar entre
corridas. Antes de deduplicar resume el historial en `primera_vista`,
`ultima_vista`, `veces_visto` y `dias_publicado`.

**La unidad de analisis no cambia.** La `clave` identifica al aviso, no al
dia: un aviso visto en tres corridas sigue siendo una fila. La definicion
pasa a ser "un aviso publicado observado entre el 7 y el 22 de septiembre".
Sigue siendo un aviso, y `clave` sigue siendo unica (verificado).

> **El supuesto que hay que declarar.** En 14 dias de inflacion argentina,
> un precio en pesos del 8 de septiembre y uno del 22 no son el mismo
> numero. Mezclar fechas mete ese sesgo en `precio_m2`, que esta en ARS para
> el 88 % de las filas. La salida elegida es dejarlo y usar `fecha_scraping`
> como control temporal, declarando la ventana. La alternativa prolija es
> deflactar por IPC, y es trabajo para la Entrega 3. Conviene decirlo antes
> de que lo pregunten.

---

## 3. El problema de fuga que hay que resolver antes que nada

Esto es lo mas importante del documento y no viene de las instrucciones: sale
de cruzar la definicion del objetivo con la tabla de columnas candidatas.

**La columna objetivo es `precio_m2`, y esta definida como
`precio / superficie_cubierta_m2`.** En `transform.py`:

```python
superficie = df["superficie_cubierta_m2"].where(df["superficie_cubierta_m2"] > 0)
df["precio_m2"] = df["precio"] / superficie
```

Entonces **`precio` no puede entrar al modelo**. No es que "ayude mucho": es
el objetivo multiplicado por una columna que tambien esta adentro. Un modelo
con `precio` y `superficie_cubierta_m2` reconstruye `precio_m2` exactamente y
da un R2 de 1,0. Es el caso de manual de la ultima columna de la tabla, y si
esta en la tabla sin marcar, la entrega se cae ahi.

La respuesta a "¿existiria al predecir?" para `precio`: **no, porque el
precio es justamente lo que se quiere evaluar**. El caso de uso es "dado un
aviso, ¿esta sobrevalorado?", y si ya se tiene el precio no hay nada que
predecir.

Conviene llegar con esto dicho antes de que lo pregunten. Es la clase de
hallazgo que muestra que el grupo entendio el problema.

**`superficie_cubierta_m2` si entra:** existe en el momento de predecir (la
publica el aviso) y no es la respuesta escrita de otra forma. Pero hay que
poder explicar por que una si y la otra no, que es exactamente la pregunta
que sigue.

---

## 4. El notebook: perfil del dataset

Crear `notebooks/entrega2_eda.ipynb`. Tiene que correr **de arriba a abajo,
sin errores y con las salidas guardadas**. Se abre antes de entrar al aula.

Las siete verificaciones que pide la catedra, en orden, cada una con la
lectura al lado (no alcanza con la salida pelada):

```python
df.shape                                         # 2504 x 47; una fila = un aviso publicado
df.dtypes.value_counts()                         # la mezcla de tipos
df.isna().mean().sort_values(ascending=False)    # las tres peores y por que
df.columns[df.nunique() <= 1]                    # condicion, provincia
df["clave"].duplicated().sum()                   # 0
df["precio_m2"].describe()                       # ver punto siguiente
df.select_dtypes("number").skew().sort_values()  # superficie 49,0
```

Ya estan todas resueltas en el notebook. `fecha_scraping` **salio** de la
lista de constantes al acumular fechas: ahora toma 4 valores.

### La distribucion del objetivo

Es, segun las instrucciones, el numero mas importante de la entrega y el que
mas se pasa por alto. `precio_m2` es continua, asi que "una clase que se come
todo" se traduce en asimetria y en escalas que conviven.

Lo que ya esta medido y hay que saber decir:

| Moneda | Filas | Mediana `precio_m2` |
|---|---|---|
| ARS | 2274 | 10.286 |
| USD | 221 | 10,81 |

**Dos poblaciones en la misma columna, con escalas que difieren por mil.** Un
promedio global de `precio_m2` no significa nada. La decision tomada:
segmentar y modelar solo ARS, que son el 91 % de las filas. La alternativa
era convertir USD a ARS con una cotizacion fija dejando registrada cual y de
que fecha, pero eso mete un supuesto de tipo de cambio que no se puede
sostener con el dato disponible.

**Los extremos hay que mirarlos, no esconderlos.** El minimo de 0,005 USD/m2
es la fila con `precio = 700` y `superficie_cubierta_m2 = 150000`: alguien
cargo la superficie del loteo en el campo del departamento. El pipeline ya la
marca en `motivo_sospecha` como "superficie fuera de rango". Esa es la
respuesta correcta a "¿por que no lo borraste?": esta marcado, es filtrable,
y la unidad de analisis es el aviso publicado tal como se publico.

### Por que hay nulos donde hay nulos

No se pide cero nulos. Se pide distinguir **faltante real** de **nulo
estructural**. En este dataset:

- `fecha_publicacion` 44,4 % nulo → **estructural**: solo InmoUP la publica.
  En las filas de Inmoclick *tiene* que estar vacia. El notebook lo muestra
  cruzando los nulos por fuente, que es la forma de probarlo y no solo
  afirmarlo.
- `valor_expensas` 42,2 % nulo → **estructural** donde `tiene_expensas` es
  `False`: un departamento sin expensas no tiene monto de expensas. Tambien
  esta cruzado en el notebook.
- `zona_escolar` 73,1 %, `plantas` 63,1 %, `piscina` 61,0 % → **faltante
  real**: el campo existe en los dos portales y el que publica no lo
  completa.
- `latitud` / `longitud` 6,3 % → **faltante real**. No se imputan: se marca
  con `tiene_geo`, porque inventar una posicion arrastra el error a las
  cuatro features derivadas.

---

## 5. Las cuatro fichas de hipotesis

Son las **seis casillas del TP2**, verificadas contra
`Documentacion/TP2 Ciencia de datos.docx`, en este orden y con estos
titulos:

| # | Campo | Que va |
|---|---|---|
| 1 | La afirmacion | Una oracion que pueda ser falsa, nombrando columnas |
| 2 | Que espero ver | Escrito **antes** de correr el codigo |
| 3 | Como la mido | Que plantilla, que medida y que grafico |
| 4 | Que encontre | El numero, su zona del semaforo y el grafico |
| 5 | Que movimiento hice | Solo si dio amarillo; si no, "ninguno" |
| 6 | Que hago con eso | La consecuencia concreta sobre el modelo |

**El campo 6 es el que se evalua de verdad:** una hipotesis que termina en
"se confirma" y no dice que cambia eso esta a la mitad.

### El semaforo del TP2

La zona no se pone a ojo: sale de la medida que corresponde a la plantilla,
y los cortes se aplican sobre el **valor absoluto** (el signo dice la
direccion y se lee aparte).

| Medida | Cuando se usa | Rojo | Amarillo | Verde |
|---|---|---|---|---|
| Separacion estandarizada | numerica entre **dos** grupos | < 0,2 | 0,2-0,8 | > 0,8 |
| eta cuadrado | numerica entre **muchos** grupos | < 0,05 | 0,05-0,25 | > 0,25 |
| Correlacion | **dos** numericas | < 0,2 | 0,2-0,6 | > 0,6 |
| Brecha Spearman - Pearson | la relacion **no es recta** | < 0,05 | 0,05-0,15 | > 0,15 |

Los movimientos (partir la poblacion, controlar una tercera variable,
cambiar la escala) **solo se habilitan en amarillo**, maximo dos, y se
eligen mirando el grafico.

Condiciones que fija la consigna:

- Al menos una que responda algo del dominio y al menos una que decida sobre
  una columna del modelo.
- **Al menos una refutada o inconclusa.** Cuatro confirmadas se lee como que
  se eligieron cosas que ya se sabian.
- Cada resultado con su numero y su zona, no solo con un grafico.
- Como maximo dos movimientos por hipotesis, justificados por el grafico.

Propuesta de las cuatro, ya apuntadas a numeros que existen:

| # | Pregunta | Plantilla | Medida | Valor | Zona | Veredicto |
|---|---|---|---|---|---|---|
| H1 | Responder | Comparacion | Separacion estandarizada | **3,02** | VERDE | Confirmada |
| H2 | Predecir | Asociacion | Brecha Spearman-Pearson | **0,107** | **AMARILLO** | Confirmada, con un movimiento |
| H3 | Responder | Asociacion | Correlacion (Spearman) | **-0,087** | ROJO | **REFUTADA** |
| H4 | Predecir | Asociacion | Correlacion (Pearson) | **0,622** | VERDE | Confirmada, al borde |

Los numeros salen del notebook ejecutado
(`notebooks/entrega2_eda.ipynb`, seccion 4), sobre el dataset final de 2504
filas y **excluyendo** las filas marcadas en `motivo_sospecha`.

**Se cumplen las condiciones de la consigna:** hay dos de responder (H1, H3)
y dos de predecir (H2, H4); hay una refutada; cada una tiene su numero y su
zona; y solo H2 lleva movimiento, uno de los dos permitidos.

> **Una diferencia con el TP2 que conviene tener contestada.** El TP2 pedia
> "no las tres con la misma plantilla", y aca hay una de comparacion (H1) y
> tres de asociacion (H2, H3, H4): no se uso **composicion**. La consigna de
> la Entrega 2 no repite esa condicion -pide al menos una de cada *pregunta*,
> no de cada *plantilla*-, y las plantillas se eligieron por la forma de cada
> afirmacion y no al reves, que es lo que el TP2 pide de fondo. Si el docente
> lo marca, la respuesta honesta es esa: ninguna de las preguntas que
> importaban para decidir columnas tenia forma de composicion.

### H3, que es la ficha para mostrar primero

**Correlacion de Spearman -0,087** entre `dist_centro_km` y `precio_m2`
(Pearson -0,040). En valor absoluto queda por debajo de 0,2: **ROJO**. Las
medianas por anillo de distancia son planas, y el anillo mas lejano tiene la
mediana mas alta, que es lo contrario de lo que afirmaba la hipotesis.

**No lleva movimiento**, y eso hay que decirlo con esas palabras: el rojo no
los habilita. Partir la poblacion hasta que el efecto aparezca es justo lo
que el TP2 advierte que no se hace.

**Campo 6:** `dist_centro_km` no entra al modelo como variable explicativa.

### La verificacion que mas conviene contar

Quedaba la duda de si **otra** codificacion de la ubicacion si explicaria el
precio. Se midio con eta cuadrado, que es la medida para comparar una
numerica entre muchos grupos:

| Agrupacion | eta cuadrado | Zona |
|---|---|---|
| `zona_geo` (6 clusters) | **0,029** | ROJA |
| `localidad` (11 departamentos) | **0,038** | ROJA |

Las dos por debajo del corte de 0,05. **La ubicacion no explica el
`precio_m2` en este dataset**, codificada de las tres formas que probamos.

Y aca esta el detalle que vale la pena tener preparado: el test de
Kruskal-Wallis entre zonas da **p < 0,000001**, o sea estadisticamente
significativo. Con 2232 filas casi cualquier diferencia lo es. El semaforo
no pregunta si el efecto existe, pregunta si es **lo bastante grande como
para cambiar una decision** — y 0,029 dice que no. Es exactamente la
distincion que el TP2 explica al justificar por que existen los cortes.

### Las otras tres

**H1 (VERDE, confirmada).** Separacion estandarizada **3,02** entre el
`precio_m2` de los avisos en ARS y en USD, contra un corte de 0,8. Dos cajas
que no se tocan. **Campo 6:** el modelo se entrena solo sobre ARS; convertir
con una cotizacion fija meteria un supuesto que el dataset no permite
justificar.

**H2 (AMARILLO, con movimiento).** Es la unica que lleva movimiento, y es el
caso de manual. La brecha Spearman-Pearson entre `superficie_cubierta_m2` y
`precio` da **0,107**: la nube sube ordenada pero **curva**, asi que Spearman
la captura y Pearson la subestima. El grafico pide **cambiar la escala**, y
no los otros dos movimientos: no hay grupos separados ni una tercera
variable en juego.

Sobre log-log la brecha cae a **-0,005** (desaparece la curvatura) y la
correlacion sube de **0,556 a 0,668**, o sea de amarillo a verde. **Campo
6:** la superficie entra al modelo como `log10(superficie)`, y eso vuelve
obligatorio tratar los 33 ceros como nulos, porque el logaritmo de cero no
existe.

**H4 (VERDE al borde, confirmada).** Pearson **0,622** entre `ambientes` y
`dormitorios`, apenas por encima del corte de 0,6. Conviene decirlo con esa
precision: es verde por 0,022, y no es una asociacion tan fuerte como para
tratarlas como la misma columna sin mirar. **Campo 6:** entra `dormitorios`
y sale `ambientes`, pero el motivo que decide no es la redundancia sino la
cobertura: **2,2 % de nulos contra 43,0 %**.

---

## 6. La tabla de columnas candidatas

Es el entregable que se lleva la Entrega 3: una fila por columna que se este
pensando dar al modelo, con las cinco columnas del ejemplo (columna, que
mide, zona, decision, ¿existiria al predecir?).

Arranque ya resuelto para los casos que no admiten discusion:

| Columna | Que mide | Zona | Decision | ¿Existiria al predecir? |
|---|---|---|---|---|
| `precio` | Precio publicado del alquiler | roja | **Sale** | **No** - es el objetivo multiplicado por la superficie (ver seccion 3) |
| `superficie_cubierta_m2` | m2 cubiertos | amarilla | Entra en escala logaritmica | Si - la publica el aviso |
| `dist_centro_km` | Distancia a Plaza Independencia | verde | Entra | Si - se deriva de lat/lon del aviso |
| `zona_geo` | Cluster geografico (6 grupos) | verde | Entra | Si |
| `dormitorios` | Cantidad de dormitorios | verde | Entra | Si |
| `fuente` | Portal de origen | verde | Entra | Si |
| `moneda` | Moneda del aviso | verde | Entra o segmenta (ver H1) | Si |
| `zona_escolar` | Zona escolar declarada | roja | Sale | Si, pero 73,1 % nulo |
| `fecha_scraping` | Fecha de la corrida que observo el aviso | amarilla | Entra como control temporal (ver el supuesto de inflacion) | Si |
| `condicion`, `provincia` | - | roja | Salen | Constantes (un solo valor: "Alquiler" y "Mendoza") |
| `clave`, `url`, `usr_id`, `prp_id`, `id_publicado` | Identificadores | roja | Salen | Si, pero no son features |
| `motivo_sospecha`, `posible_duplicado_cruzado` | Marcas de calidad del pipeline | - | No son features: son filtros de fila | - |

> **Si preguntan por los 363 duplicados cruzados.** Son firmas que
> aparecen en los dos portales, y la marca **sobreestima a proposito**. La
> firma es direccion + localidad + superficie, y muchos avisos publican la
> direccion sin numero ("FRENTE AL DALVIAN") o con el del edificio: dos
> departamentos distintos de la misma torre y la misma tipologia caen en la
> misma firma. Hay grupos de 4 y 5 avisos con precios distintos, que no son
> el mismo inmueble repetido sino el mismo edificio. Por eso la columna se
> llama `posible_duplicado_cruzado`, y por eso marca en vez de borrar.

### El resto de las columnas

Porcentajes de nulos medidos sobre las **2504 filas** del dataset final
(`resultados/20260922/`).

| Columna | Que mide | Nulos | Zona | Decision | ¿Existiria al predecir? |
|---|---|---|---|---|---|
| `banos` | Cantidad de banos | 0,2 % | verde | Entra | Si |
| `superficie_total_m2` | m2 totales | 4,5 % | verde | Entra (log, misma cola que la cubierta) | Si |
| `localidad` | Departamento de Mendoza | 0,0 % | verde | Entra, aunque `zona_geo` la mejora | Si |
| `tipo_anunciante` | Inmobiliaria o dueno directo | 0,0 % | verde | Entra | Si |
| `es_dueno_directo` | Idem, en booleano | 0,0 % | amarilla | Redundante con `tipo_anunciante`: entra una | Si |
| `estado_conservacion` | Bueno / a refaccionar / ... | 6,0 % | verde | Entra | Si |
| `cochera` | Con o sin cochera | 9,0 % | amarilla | Entra, pero hoy es texto con 7 valores: hay que normalizarla | Si |
| `antiguedad` | Anos de la propiedad | 43,7 % | amarilla | Entra con marca de faltante | Si |
| `acepta_mascotas` | Acepta mascotas | 21,9 % | amarilla | Entra como tres estados (si / no / sin dato) | Si |
| `tiene_expensas` | Si paga expensas | 37,2 % | amarilla | Entra como tres estados | Si |
| `valor_expensas` | Monto de expensas | 42,2 % | amarilla | Entra; el nulo es **estructural** donde no hay expensas | Si |
| `amoblado` | Amoblado | 53,9 % | roja | Sale: mas de la mitad sin dato | Si |
| `tipo_construccion` | Departamento, PH, ... | 55,8 % | roja | Sale: paso de 47 % a 56 % de nulos al sumar InmoUP | Si |
| `piscina` | Tiene piscina | 61,0 % | roja | Sale | Si |
| `plantas` | Cantidad de plantas | 63,1 % | roja | Sale | Si |
| `zona_escolar` | Zona escolar declarada | 73,1 % | roja | Sale | Si |
| `ambientes` | Cantidad de ambientes | 43,0 % | roja | Sale: se queda `dormitorios` (2,2 % de nulos). Ver H4 | Si |
| `latitud`, `longitud` | Coordenadas crudas | 6,3 % | roja | Salen: entran a traves de `dist_centro_km` y `zona_geo` | Si |
| `direccion` | Direccion publicada | 0,1 % | roja | Sale: 2182 valores distintos en 2504 filas | Si |
| `publicador` | Nombre de quien publica | 0,0 % | roja | Sale: 791 categorias. Entra via `tipo_anunciante` | Si |
| `descripcion` | Texto libre del aviso | 0,0 % | roja | Sale de esta entrega (2453 textos distintos). Material de NLP para mas adelante | Si |
| `fecha_publicacion` | Cuando se publico | 44,4 % | roja | Sale: solo la publica InmoUP (nulo **estructural**) | Si |
| `tiene_geo` | Si el aviso trae coordenadas | 0,0 % | verde | Entra: marca el 6,3 % sin geo en vez de imputarlo | Si |

> **Un cambio que trajo el dataset nuevo:** `tipo_construccion` paso de
> 47,3 % a **55,8 %** de nulos al entrar mas avisos de InmoUP, y con eso
> cruzo la mitad y se fue a zona roja. Es un buen ejemplo de por que la
> tabla se revisa con cada corrida y no se escribe una vez.

### Las dos features que hay que mirar con lupa

`dias_publicado` y `veces_visto` salen de la acumulacion de fechas, y son el
caso de fuga mas sutil del dataset.

**Conceptualmente son la mejor variable del proyecto.** Un aviso que lleva
dos meses publicado y no se alquila es, casi por definicion, un aviso
sobrevalorado, que es exactamente la pregunta del proyecto.

**Pero hoy estan mal medidas.** Solo observamos del 7 al 22 de septiembre,
asi que `dias_publicado` no puede pasar de 15 y `veces_visto` no puede pasar
de 4, por construccion y no porque el mercado sea asi. Un aviso publicado en
julio figura con los mismos "0 dias" que uno de ayer si aparecio recien en
nuestra primera corrida. Es una variable **censurada por la ventana de
observacion**.

Y hay un segundo problema, de fuga: al predecir sobre un aviso **recien
publicado**, `dias_publicado` vale 0 y `veces_visto` vale 1 siempre. Si el
modelo aprende a apoyarse en ellas, no las va a tener cuando mas las
necesita.

**Decision:** zona roja para la Entrega 3, y se explica por que. Vuelven a
estar sobre la mesa cuando el scheduling diario lleve algunos meses
corriendo y la ventana sea lo bastante larga como para que el numero
signifique algo.

**Cada fila tiene que pasar el chequeo de fuga**, que es la que mas caro se
paga si falta.

---

## 7. Reparto y ensayo

La nota es individual y las preguntas van dirigidas. **Que hable uno solo es,
segun la consigna, el error mas caro.**

Reparto sugerido, con la regla de que cada uno tiene que poder contestar por
lo del otro:

| Integrante | Prepara | Tiene que poder defender tambien |
|---|---|---|
| 1 | Scheduling (seccion 1) + los 2 minutos de "que cambio" | Por que `catchup=False` |
| 2 | Features geograficas (seccion 2) | Por que el `precio_m2` del vecindario es fuga |
| 3 | Perfil + distribucion del objetivo (seccion 4) | Los nulos estructurales vs reales |
| 4 | Fichas H1-H4 (seccion 5) | La ficha refutada, entera |
| Todos | Tabla de columnas candidatas (seccion 6) | La fila de `precio` |

**Ensayar los 20 minutos con reloj**, respetando el reparto de la consigna:
2 min que cambio, 5 min perfil, 6 min dos hipotesis que elige el docente
(por eso las cuatro tienen que estar igual de preparadas), 5 min preguntas
dirigidas, 2 min devolucion.

### Guion de los 20 minutos

**0-2 min - Que cambio (dos oraciones, no mas).**

> "De la Entrega 1 nos marcaron dos cosas: que el DAG no tenia scheduling y
> que teniamos latitud y longitud sin usar. Las dos estan resueltas: el DAG
> corre diario a las 6 y las coordenadas ahora son cuatro features. Ademas
> cambiamos algo que no nos habian marcado: el dataset acumula fechas, porque
> nos dimos cuenta de que cada scrapeo es una foto y no un incremento."

La pregunta del proyecto no cambio.

**2-7 min - El perfil.** Notebook abierto, seccion 1. En orden:

1. 2504 filas x 47 columnas. Una fila es un aviso, no una propiedad.
2. La clave `fuente:usr_id-prp_id` da 0 duplicados, y **por que hace falta
   el nombre de la fuente**: el numero del aviso se repite entre
   inmobiliarias y entre portales.
3. Los nulos, separando **estructural** de **faltante real** (mostrar el
   cruce de `fecha_publicacion` por fuente: es la prueba, no la afirmacion).
4. **La distribucion del objetivo**, que es el punto que mas pesa: dos
   poblaciones por moneda, 952 a 1. Mostrar el histograma lineal al lado del
   log.
5. Los extremos: la fila de 150.000 m2, marcada y no borrada.

**7-13 min - Dos hipotesis, las que elija el docente.** Las cuatro estan
igual de preparadas. Si hay margen para sugerir, **empezar por H3**: es la
refutada con el numero mas claro.

Cada ficha se recorre entera: afirmacion, como se midio, el numero, la zona,
el grafico, la decision. **La decision es lo que se corrige**, no la
cantidad de graficos.

**13-18 min - Preguntas dirigidas.** Las cinco que mas probablemente caigan,
con quien las contesta:

| Pregunta probable | Respuesta corta | Quien |
|---|---|---|
| ¿Cual es la variable objetivo y como esta distribuida? | `precio_m2`, bimodal por moneda, asimetrica; se modela sobre ARS | 3 |
| ¿Por que `precio` no entra al modelo? | Es el objetivo multiplicado por la superficie; error de reconstruccion 10^-12 | Todos |
| ¿Por que hay nulos en `valor_expensas`? | Estructural: sin expensas no hay monto. Cruzado en el notebook | 3 |
| ¿Que hipotesis te salio mal y que hiciste? | H3: Spearman -0,087, ROJO, refutada. **Ningun movimiento: el rojo no los habilita**. Despues verificamos aparte que la zona tampoco explica (eta2 0,029) | 4 |
| ¿Por que `zona_geo` no entra si el test da significativo? | Kruskal da p<0,000001 pero eta2 = 0,029: con 2232 filas casi todo es significativo; el semaforo mide si el efecto es **grande**, no si existe | 2 |
| ¿Por que H2 lleva movimiento y las otras no? | Fue la unica que dio amarillo (brecha 0,107). El grafico mostraba una nube curva, que es el caso en que el TP2 indica **cambiar la escala** | 4 |
| ¿Por que `catchup=False`? | Los portales muestran solo lo publicado hoy; un backfill guardaria N veces el mismo inventario | 1 |

**18-20 min - Devolucion.** Anotarla. Es la materia prima de la Entrega 3.

### Si algo sale mal

- **Si preguntan por una columna que no esta en la tabla:** la tabla tiene
  las 47. Si igual aparece una, la respuesta honesta es "esa no la
  analizamos", no una justificacion improvisada.
- **Si el notebook no abre:** esta ejecutado y con las salidas guardadas, asi
  que VS Code lo muestra sin kernel. Igual conviene tenerlo abierto **antes**
  de entrar.
- **Si preguntan por que el dataset tiene fechas mezcladas:** ventana del 7
  al 22 de septiembre, declarada, con el supuesto de inflacion dicho de
  frente (seccion 2 bis).

---

## 8. Estado

| Que | Estado | Donde |
|---|---|---|
| Scheduling | **Hecho** | `dags/brujula_pipeline.py` |
| Features geograficas | **Hecho** | `dags/brujula/transform.py` |
| Dataset acumulado por fechas | **Hecho** | `parse.py` + `transform.py` |
| Notebook ejecutado con salidas | **Hecho** | `notebooks/entrega2_eda.ipynb` |
| Perfil del dataset (7 verificaciones) | **Hecho** | Notebook, seccion 1 |
| Distribucion del objetivo | **Hecho** | Notebook, seccion 1.6 |
| Las cuatro fichas | **Hecho**, con las 6 casillas del TP2 y el semaforo aplicado | Notebook, seccion 4 |
| Tabla de columnas candidatas | **Hecho** (47/47 columnas, verificado) | Notebook seccion 5 + este archivo seccion 6 |
| Chequeo de fuga | **Hecho** (dos fugas encontradas) | Secciones 3 y 6 |
| Que quedo inconcluso | **Hecho** (5 puntos) | Notebook, seccion 6 |
| Reporte de calidad | **Hecho** | `resultados/<fecha>/quality_report.txt` |
| Ensayo con reloj | **Pendiente: lo hace el grupo** | Seccion 7 |
| Plantilla exacta del TP2 | **Hecho**: verificada contra el docx | Notebook, seccion 4 |
| Que cambio desde la Entrega 1 | **Hecho** | Notebook, seccion 0 |

### El unico pendiente real

**El ensayo.** 20 minutos con reloj, con el reparto de la seccion 7. Es
lo unico que no se puede dejar hecho de antemano, y segun la consigna "que
hable uno solo" es el error mas caro.

> De la consigna: "Si algo no te dio, veni igual y decilo. Un grupo que
> muestra un analisis a medias y sabe donde esta trabado se lleva una
> devolucion util."
