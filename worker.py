"""Worker: nodo que ejecuta tareas recibidas por socket.

Cada worker es un servidor TCP propio con un pool de hilos fijo
(ThreadPoolExecutor). Se pueden levantar tantos workers como se quiera, en
la misma máquina (puertos distintos) o en máquinas distintas.
"""

import argparse
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import protocolo
from tareas import TAREAS

log = logging.getLogger("worker")


class Worker:
    def __init__(self, nombre: str, host: str = "127.0.0.1", puerto: int = 0, hilos: int = 4):
        self.nombre = nombre
        self.host = host
        self.hilos = hilos
        self._lock = threading.Lock()
        self.en_curso = 0
        self.completadas = 0
        self._detenido = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=nombre)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((host, puerto))
        self._socket.listen(64)
        self._socket.settimeout(0.5)  # para poder revisar la bandera de parada
        self.puerto = self._socket.getsockname()[1]
        self._hilo_aceptador = None

    def iniciar(self) -> None:
        self._hilo_aceptador = threading.Thread(
            target=self.servir, name=f"{self.nombre}-accept", daemon=True
        )
        self._hilo_aceptador.start()

    def servir(self) -> None:
        log.info("%s escuchando en %s:%s (pool de %s hilos)", self.nombre, self.host, self.puerto, self.hilos)
        while not self._detenido.is_set():
            try:
                conexion, direccion = self._socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self._pool.submit(self._atender, conexion, direccion)

    def detener(self) -> None:
        self._detenido.set()
        self._socket.close()
        if self._hilo_aceptador is not None:
            self._hilo_aceptador.join(timeout=2)
        self._pool.shutdown(wait=False, cancel_futures=True)
        log.info("%s detenido", self.nombre)

    def _atender(self, conexion: socket.socket, direccion) -> None:
        with conexion:
            conexion.settimeout(30)
            try:
                with conexion.makefile("rb") as archivo:
                    mensaje = protocolo.recibir(archivo)
                    if mensaje is None:
                        return
                    protocolo.enviar(conexion, self._responder(mensaje))
            except protocolo.ProtocoloError as error:
                self._responder_error(conexion, str(error))
            except OSError as error:
                log.warning("%s: conexión con %s interrumpida: %s", self.nombre, direccion, error)

    @staticmethod
    def _responder_error(conexion: socket.socket, texto: str) -> None:
        try:
            protocolo.enviar(conexion, {"ok": False, "error": texto})
        except OSError:
            pass

    def _responder(self, mensaje: dict) -> dict:
        accion = mensaje.get("accion")
        if accion == "ping":
            with self._lock:
                return {"ok": True, "worker": self.nombre, "en_curso": self.en_curso}
        if accion != "ejecutar":
            return {"ok": False, "error": f"acción desconocida: {accion!r}"}

        funcion = TAREAS.get(mensaje.get("tipo"))
        if funcion is None:
            return {"ok": False, "error": f"tipo de tarea desconocido: {mensaje.get('tipo')!r}"}

        with self._lock:
            self.en_curso += 1
        inicio = time.perf_counter()
        try:
            resultado = funcion(mensaje.get("datos"))
            respuesta = {"ok": True, "resultado": resultado}
        except ValueError as error:
            respuesta = {"ok": False, "error": str(error)}
        finally:
            with self._lock:
                self.en_curso -= 1
                self.completadas += 1
        respuesta["worker"] = self.nombre
        respuesta["ms"] = round((time.perf_counter() - inicio) * 1000, 2)
        log.info("%s ejecutó %s -> %s (%s ms)", self.nombre, mensaje.get("tipo"),
                 "ok" if respuesta["ok"] else respuesta["error"], respuesta["ms"])
        return respuesta


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker del sistema distribuido (PFO 3)")
    parser.add_argument("--nombre", default="worker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--puerto", type=int, default=6001)
    parser.add_argument("--hilos", type=int, default=4, help="tamaño del pool de hilos")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    worker = Worker(args.nombre, args.host, args.puerto, args.hilos)
    try:
        worker.servir()
    except KeyboardInterrupt:
        pass
    finally:
        worker.detener()


if __name__ == "__main__":
    main()
