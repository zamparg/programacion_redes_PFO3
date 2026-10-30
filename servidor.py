"""Servidor (gateway/coordinador): recibe tareas por socket y las distribuye a workers.

Flujo de una tarea:
  cliente --socket--> hilo de conexión --> cola de tareas --> despachador
          --socket--> worker elegido (menos carga) --> resultado --> cliente

- Pool de hilos para atender conexiones de clientes.
- Cola acotada (queue.Queue): hace de cola de mensajes (en producción sería
  RabbitMQ) y aplica contrapresión si el sistema se satura.
- Despachadores: hilos que sacan tareas de la cola y las envían a un worker.
  Si el worker no responde, se lo marca caído un tiempo y se reintenta en otro.
- Usuarios (registro e inicio de sesión) y tareas persistidos en SQLite (en producción, PostgreSQL).
"""

import argparse
import hmac
import itertools
import json
import logging
import os
import queue
import socket
import sqlite3
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

import protocolo
import usuarios
from tareas import TAREAS

log = logging.getLogger("servidor")

_ERROR_CREDENCIALES = (
    'Se requieren "usuario" y "contraseña" como texto no vacío; el usuario no puede contener ":"'
)

ESQUEMA = """
CREATE TABLE IF NOT EXISTS tareas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario TEXT,
    tipo TEXT NOT NULL,
    datos TEXT,
    estado TEXT NOT NULL,
    resultado TEXT,
    error TEXT,
    worker TEXT,
    ms REAL,
    creada TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finalizada TEXT
)
"""


@dataclass(eq=False)
class WorkerRemoto:
    host: str
    puerto: int
    en_curso: int = 0
    completadas: int = 0
    caido_hasta: float = 0.0
    ultimo_uso: int = 0
    nombre: str = field(default="")

    def __post_init__(self):
        self.nombre = self.nombre or f"{self.host}:{self.puerto}"


@dataclass(eq=False)
class Tarea:
    id: int
    tipo: str
    datos: object
    futuro: Future = field(default_factory=Future)


def _credenciales(mensaje: dict) -> tuple[str, str] | None:
    """Devuelve (usuario, contraseña) limpios, o None si el formato no es válido."""
    usuario, contraseña = mensaje.get("usuario"), mensaje.get("contraseña")
    if not isinstance(usuario, str) or not isinstance(contraseña, str):
        return None
    usuario = usuario.strip()
    if not usuario or not contraseña or ":" in usuario:
        return None
    return usuario, contraseña


class Servidor:
    def __init__(
        self,
        workers: list[tuple[str, int]],
        host: str = "127.0.0.1",
        puerto: int = 5050,
        db: str | Path = "tareas.db",
        hilos_clientes: int = 16,
        despachadores: int = 8,
        capacidad_cola: int = 1000,
        timeout_cliente: float = 60,
        timeout_worker: float = 30,
        enfriamiento: float = 3,
        token: str | None = None,
    ):
        if not workers:
            raise ValueError("se necesita al menos un worker")
        self.workers = [WorkerRemoto(h, p) for h, p in workers]
        self.host = host
        self.db = str(db)
        self.timeout_cliente = timeout_cliente
        self.timeout_worker = timeout_worker
        self.enfriamiento = enfriamiento
        self.token = token
        self.n_despachadores = despachadores

        self._cola: queue.Queue[Tarea] = queue.Queue(maxsize=capacidad_cola)
        self._lock = threading.Lock()  # protege contadores y estado de workers
        self._lock_db = threading.Lock()
        self._turno = itertools.count(1)
        self._recibidas = 0
        self._detenido = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=hilos_clientes, thread_name_prefix="cliente")
        self._hilos: list[threading.Thread] = []

        self._iniciar_db()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, puerto))
        self._socket.listen(128)
        self._socket.settimeout(0.5)
        self.puerto = self._socket.getsockname()[1]

        for i in range(self.n_despachadores):
            self._lanzar(self._despachar, f"despachador-{i + 1}")

    # ---------- ciclo de vida ----------

    def iniciar(self) -> None:
        """Atiende conexiones en segundo plano (servir() lo hace bloqueando)."""
        self._lanzar(self.servir, "aceptador")

    def _lanzar(self, destino, nombre: str) -> None:
        hilo = threading.Thread(target=destino, name=nombre, daemon=True)
        hilo.start()
        self._hilos.append(hilo)

    def servir(self) -> None:
        """Bucle de aceptación; bloquea hasta detener()."""
        log.info("Servidor escuchando en %s:%s con %s workers", self.host, self.puerto, len(self.workers))
        while not self._detenido.is_set():
            try:
                conexion, direccion = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self._pool.submit(self._atender_cliente, conexion, direccion)

    def detener(self) -> None:
        self._detenido.set()
        self._socket.close()
        for hilo in self._hilos:
            hilo.join(timeout=2)
        self._pool.shutdown(wait=False, cancel_futures=True)
        log.info("Servidor detenido")

    # ---------- base de datos ----------

    def _iniciar_db(self) -> None:
        Path(self.db).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.db)) as con:
            con.execute(usuarios.ESQUEMA)
            con.execute(ESQUEMA)
            columnas = {fila[1] for fila in con.execute("PRAGMA table_info(tareas)")}
            if "usuario" not in columnas:  # bases creadas antes de existir los usuarios
                con.execute("ALTER TABLE tareas ADD COLUMN usuario TEXT")
            con.commit()

    def _ejecutar_sql(self, sql: str, params: tuple = ()) -> int:
        with self._lock_db, closing(sqlite3.connect(self.db)) as con:
            cursor = con.execute(sql, params)
            con.commit()
            return cursor.lastrowid

    def _consultar_tarea(self, id_tarea: int, usuario: str) -> dict | None:
        with closing(sqlite3.connect(self.db)) as con:
            con.row_factory = sqlite3.Row
            fila = con.execute(
                "SELECT * FROM tareas WHERE id = ? AND usuario = ?", (id_tarea, usuario)
            ).fetchone()
        if fila is None:
            return None
        tarea = dict(fila)
        for clave in ("datos", "resultado"):
            if tarea[clave] is not None:
                tarea[clave] = json.loads(tarea[clave])
        return tarea

    # ---------- atención de clientes ----------

    def _atender_cliente(self, conexion: socket.socket, direccion) -> None:
        log.info("Conexión de %s:%s", *direccion)
        with conexion:
            conexion.settimeout(self.timeout_cliente)
            with conexion.makefile("rb") as archivo:
                while not self._detenido.is_set():
                    try:
                        mensaje = protocolo.recibir(archivo)
                        if mensaje is None:
                            break
                        respuesta = self._procesar(mensaje)
                    except protocolo.LineaDemasiadoLarga as error:
                        self._intentar_enviar(conexion, {"ok": False, "error": str(error)})
                        break
                    except protocolo.ProtocoloError as error:
                        respuesta = {"ok": False, "error": str(error)}
                    except socket.timeout:
                        log.info("Cliente %s:%s inactivo; se cierra la conexión", *direccion)
                        break
                    except OSError:
                        break
                    if not self._intentar_enviar(conexion, respuesta):
                        break
        log.info("Conexión cerrada con %s:%s", *direccion)

    @staticmethod
    def _intentar_enviar(conexion: socket.socket, mensaje: dict) -> bool:
        try:
            protocolo.enviar(conexion, mensaje)
            return True
        except OSError:
            return False

    def _procesar(self, mensaje: dict) -> dict:
        if self.token is not None:
            recibido = str(mensaje.get("token", ""))
            if not hmac.compare_digest(recibido.encode(), self.token.encode()):
                return {"ok": False, "error": "no autorizado: token incorrecto"}

        accion = mensaje.get("accion")
        if accion == "registro":
            return self._accion_registro(mensaje)
        if accion == "login":
            return self._accion_login(mensaje)
        if accion == "enviar":
            return self._enviar_tarea(mensaje)
        if accion == "consultar":
            return self._accion_consultar(mensaje)
        if accion == "estado":
            return self._accion_estado()
        if accion == "tipos":
            return {"ok": True, "tipos": sorted(TAREAS)}
        return {"ok": False, "error": f"acción desconocida: {accion!r}"}

    # ---------- usuarios ----------

    def _accion_registro(self, mensaje: dict) -> dict:
        credenciales = _credenciales(mensaje)
        if credenciales is None:
            return {"ok": False, "codigo": 400, "error": _ERROR_CREDENCIALES}
        if not usuarios.registrar(self.db, *credenciales):
            return {"ok": False, "codigo": 409, "error": "El usuario ya existe"}
        return {"ok": True, "codigo": 201, "mensaje": "Usuario registrado correctamente"}

    def _accion_login(self, mensaje: dict) -> dict:
        credenciales = _credenciales(mensaje)
        if credenciales is None:
            return {"ok": False, "codigo": 400, "error": _ERROR_CREDENCIALES}
        usuario, contraseña = credenciales
        if not usuarios.verificar(self.db, usuario, contraseña):
            return {"ok": False, "codigo": 401, "error": "Usuario o contraseña incorrectos"}
        return {"ok": True, "codigo": 200, "usuario": usuario, "mensaje": "Inicio de sesión correcto"}

    def _autenticar(self, mensaje: dict) -> tuple[str | None, dict | None]:
        """Devuelve (usuario, None) si las credenciales son válidas, o (None, error)."""
        credenciales = _credenciales(mensaje)
        if credenciales is None:
            return None, {"ok": False, "codigo": 401, "error": "Autenticación requerida: envíe usuario y contraseña"}
        usuario, contraseña = credenciales
        if not usuarios.verificar(self.db, usuario, contraseña):
            return None, {"ok": False, "codigo": 401, "error": "Usuario o contraseña incorrectos"}
        return usuario, None

    # ---------- tareas ----------

    def _enviar_tarea(self, mensaje: dict) -> dict:
        usuario, error = self._autenticar(mensaje)
        if error is not None:
            return error
        tipo = mensaje.get("tipo")
        if tipo not in TAREAS:
            return {"ok": False, "error": f"tipo de tarea desconocido: {tipo!r}"}
        datos = mensaje.get("datos")

        id_tarea = self._ejecutar_sql(
            "INSERT INTO tareas (usuario, tipo, datos, estado) VALUES (?, ?, ?, 'pendiente')",
            (usuario, tipo, json.dumps(datos, ensure_ascii=False)),
        )
        tarea = Tarea(id_tarea, tipo, datos)
        try:
            self._cola.put_nowait(tarea)
        except queue.Full:
            self._ejecutar_sql(
                "UPDATE tareas SET estado='error', error=?, finalizada=CURRENT_TIMESTAMP WHERE id=?",
                ("servidor saturado", id_tarea),
            )
            return {"ok": False, "id": id_tarea, "error": "servidor saturado, reintente más tarde"}
        with self._lock:
            self._recibidas += 1

        try:
            respuesta = tarea.futuro.result(timeout=mensaje.get("timeout", 120))
        except FutureTimeout:
            return {"ok": False, "id": id_tarea, "error": "tiempo de espera agotado; consulte el id más tarde"}
        respuesta["id"] = id_tarea
        return respuesta

    def _accion_consultar(self, mensaje: dict) -> dict:
        usuario, error = self._autenticar(mensaje)
        if error is not None:
            return error
        id_tarea = mensaje.get("id")
        if isinstance(id_tarea, bool) or not isinstance(id_tarea, int):
            return {"ok": False, "error": 'se requiere "id" entero'}
        tarea = self._consultar_tarea(id_tarea, usuario)
        if tarea is None:
            return {"ok": False, "error": f"no existe la tarea {id_tarea}"}
        return {"ok": True, "tarea": tarea}

    def _accion_estado(self) -> dict:
        ahora = time.monotonic()
        with self._lock:
            workers = [
                {
                    "nombre": w.nombre,
                    "activo": w.caido_hasta <= ahora,
                    "en_curso": w.en_curso,
                    "completadas": w.completadas,
                }
                for w in self.workers
            ]
            recibidas = self._recibidas
        return {"ok": True, "en_cola": self._cola.qsize(), "recibidas": recibidas, "workers": workers}

    # ---------- distribución a workers ----------

    def _elegir_worker(self, excluidos: set) -> WorkerRemoto | None:
        """Menos tareas en curso; a igualdad, el menos recientemente usado."""
        with self._lock:
            ahora = time.monotonic()
            libres = [w for w in self.workers if w.caido_hasta <= ahora and w not in excluidos]
            if not libres:
                return None
            elegido = min(libres, key=lambda w: (w.en_curso, w.ultimo_uso))
            elegido.en_curso += 1
            elegido.ultimo_uso = next(self._turno)
            return elegido

    def _despachar(self) -> None:
        while not self._detenido.is_set():
            try:
                tarea = self._cola.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                respuesta = self._ejecutar_en_worker(tarea)
            except Exception:  # un fallo inesperado no debe dejar al cliente colgado
                log.exception("Error inesperado procesando la tarea %s", tarea.id)
                respuesta = {"ok": False, "error": "error interno del servidor"}
            self._registrar_resultado(tarea, respuesta)
            tarea.futuro.set_result(respuesta)

    def _ejecutar_en_worker(self, tarea: Tarea) -> dict:
        excluidos: set = set()
        pedido = {"accion": "ejecutar", "tipo": tarea.tipo, "datos": tarea.datos}
        while True:
            worker = self._elegir_worker(excluidos)
            if worker is None:
                return {"ok": False, "error": "no hay workers disponibles"}
            try:
                respuesta = protocolo.peticion(worker.host, worker.puerto, pedido, self.timeout_worker)
            except (OSError, protocolo.ProtocoloError) as error:
                log.warning("Worker %s falló (%s); se reintenta en otro", worker.nombre, error)
                excluidos.add(worker)
                with self._lock:
                    worker.en_curso -= 1
                    worker.caido_hasta = time.monotonic() + self.enfriamiento
                continue
            with self._lock:
                worker.en_curso -= 1
                worker.completadas += 1
            return respuesta

    def _registrar_resultado(self, tarea: Tarea, respuesta: dict) -> None:
        if respuesta.get("ok"):
            self._ejecutar_sql(
                "UPDATE tareas SET estado='completada', resultado=?, worker=?, ms=?, "
                "finalizada=CURRENT_TIMESTAMP WHERE id=?",
                (json.dumps(respuesta.get("resultado"), ensure_ascii=False),
                 respuesta.get("worker"), respuesta.get("ms"), tarea.id),
            )
        else:
            self._ejecutar_sql(
                "UPDATE tareas SET estado='error', error=?, worker=?, finalizada=CURRENT_TIMESTAMP WHERE id=?",
                (respuesta.get("error"), respuesta.get("worker"), tarea.id),
            )


def parsear_workers(texto: str) -> list[tuple[str, int]]:
    resultado = []
    for parte in texto.split(","):
        host, _, puerto = parte.strip().rpartition(":")
        if not host or not puerto.isdigit():
            raise argparse.ArgumentTypeError(f"worker inválido {parte!r}; use host:puerto")
        resultado.append((host, int(puerto)))
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor distribuidor de tareas (PFO 3)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--puerto", type=int, default=5050)
    parser.add_argument("--workers", type=parsear_workers,
                        default="127.0.0.1:6001,127.0.0.1:6002,127.0.0.1:6003",
                        help="lista host:puerto separada por comas")
    parser.add_argument("--db", default=str(Path(__file__).with_name("tareas.db")))
    parser.add_argument("--token", default=os.environ.get("PFO3_TOKEN"),
                        help="si se define, los clientes deben enviarlo (o variable PFO3_TOKEN)")
    args = parser.parse_args()
    if isinstance(args.workers, str):
        args.workers = parsear_workers(args.workers)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    servidor = Servidor(args.workers, args.host, args.puerto, args.db, token=args.token)
    try:
        servidor.servir()
    except KeyboardInterrupt:
        pass
    finally:
        servidor.detener()


if __name__ == "__main__":
    main()
