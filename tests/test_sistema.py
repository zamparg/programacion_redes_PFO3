import json
import logging
import shutil
import socket
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import protocolo  # noqa: E402
import tareas  # noqa: E402
from cliente import Cliente, interpretar_datos  # noqa: E402
from servidor import Servidor  # noqa: E402
from worker import Worker  # noqa: E402

logging.disable(logging.CRITICAL)


class TestTareas(unittest.TestCase):
    def test_resultados_correctos(self):
        self.assertEqual(tareas.fibonacci(10), 55)
        self.assertEqual(tareas.factorial(5), 120)
        self.assertTrue(tareas.es_primo(97)["es_primo"])
        self.assertFalse(tareas.es_primo(91)["es_primo"])
        self.assertEqual(tareas.contar_primos(100)["cantidad"], 25)
        self.assertEqual(tareas.invertir("abc"), "cba")
        self.assertEqual(tareas.sumar([1, 2, 3.5]), 6.5)

    def test_datos_invalidos(self):
        with self.assertRaises(ValueError):
            tareas.fibonacci("abc")
        with self.assertRaises(ValueError):
            tareas.fibonacci(10**9)
        with self.assertRaises(ValueError):
            tareas.sumar(["a"])

    def test_interpretar_datos(self):
        self.assertEqual(interpretar_datos("30"), 30)
        self.assertEqual(interpretar_datos("[1, 2]"), [1, 2])
        self.assertEqual(interpretar_datos("hola mundo"), "hola mundo")


class SistemaBase(unittest.TestCase):
    """Levanta 3 workers y un servidor en puertos libres, todo en el mismo proceso."""

    token = None

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.workers = [Worker(f"w{i}", puerto=0, hilos=4) for i in range(1, 4)]
        for w in self.workers:
            w.iniciar()
        self.servidor = Servidor(
            [("127.0.0.1", w.puerto) for w in self.workers],
            puerto=0,
            db=Path(self.tmp) / "test.db",
            enfriamiento=0.2,
            timeout_cliente=2,
            token=self.token,
        )
        self.servidor.iniciar()
        self.cliente = Cliente("127.0.0.1", self.servidor.puerto, token=self.token, timeout=10)

    def tearDown(self):
        self.servidor.detener()
        for w in self.workers:
            w.detener()
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestSistema(SistemaBase):
    def test_enviar_y_recibir_resultado(self):
        r = self.cliente.enviar("fibonacci", 30)
        self.assertTrue(r["ok"])
        self.assertEqual(r["resultado"], 832040)
        self.assertIn(r["worker"], {"w1", "w2", "w3"})

    def test_tipo_y_datos_invalidos(self):
        self.assertFalse(self.cliente.enviar("inexistente", 1)["ok"])
        r = self.cliente.enviar("fibonacci", "abc")
        self.assertFalse(r["ok"])
        self.assertIn("entero", r["error"])

    def test_tareas_concurrentes_se_reparten_entre_workers(self):
        with ThreadPoolExecutor(max_workers=12) as pool:
            respuestas = list(pool.map(lambda _: self.cliente.enviar("dormir", 0.3), range(12)))
        self.assertTrue(all(r["ok"] for r in respuestas))
        usados = {r["worker"] for r in respuestas}
        self.assertEqual(usados, {"w1", "w2", "w3"})
        self.assertEqual(len({r["id"] for r in respuestas}), 12)

    def test_concurrencia_real(self):
        """12 tareas de 0.3 s en 3 workers x 4 hilos deben tardar mucho menos que 3.6 s."""
        inicio = time.perf_counter()
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(lambda _: self.cliente.enviar("dormir", 0.3), range(12)))
        self.assertLess(time.perf_counter() - inicio, 2.0)

    def test_tolerancia_a_fallos_de_un_worker(self):
        self.workers[0].detener()
        respuestas = [self.cliente.enviar("factorial", 10) for _ in range(9)]
        self.assertTrue(all(r["ok"] for r in respuestas))
        self.assertNotIn("w1", {r["worker"] for r in respuestas})
        estado = self.cliente.estado()
        self.assertEqual(sum(1 for w in estado["workers"] if not w["activo"]), 1)

    def test_sin_workers_disponibles(self):
        for w in self.workers:
            w.detener()
        r = self.cliente.enviar("factorial", 5)
        self.assertFalse(r["ok"])
        self.assertIn("no hay workers", r["error"])

    def test_resultado_persistido_y_consultable(self):
        r = self.cliente.enviar("invertir", "hola")
        consulta = self.cliente.consultar(r["id"])
        self.assertTrue(consulta["ok"])
        self.assertEqual(consulta["tarea"]["estado"], "completada")
        self.assertEqual(consulta["tarea"]["resultado"], "aloh")
        self.assertFalse(self.cliente.consultar(9999)["ok"])

    def test_estado_y_tipos(self):
        self.cliente.enviar("factorial", 5)
        estado = self.cliente.estado()
        self.assertEqual(len(estado["workers"]), 3)
        self.assertEqual(estado["recibidas"], 1)
        self.assertIn("fibonacci", self.cliente.tipos())

    def test_varias_tareas_en_una_misma_conexion(self):
        with socket.create_connection(("127.0.0.1", self.servidor.puerto), timeout=5) as sock:
            with sock.makefile("rb") as archivo:
                for n in (5, 6, 7):
                    protocolo.enviar(sock, {"accion": "enviar", "tipo": "factorial", "datos": n})
                    self.assertTrue(protocolo.recibir(archivo)["ok"])

    def test_json_invalido_no_cierra_la_conexion(self):
        with socket.create_connection(("127.0.0.1", self.servidor.puerto), timeout=5) as sock:
            with sock.makefile("rb") as archivo:
                sock.sendall(b"esto no es json\n")
                self.assertFalse(protocolo.recibir(archivo)["ok"])
                protocolo.enviar(sock, {"accion": "tipos"})
                self.assertTrue(protocolo.recibir(archivo)["ok"])

    def test_timeout_por_inactividad(self):
        with socket.create_connection(("127.0.0.1", self.servidor.puerto), timeout=6) as sock:
            with sock.makefile("rb") as archivo:
                self.assertIsNone(protocolo.recibir(archivo))  # el servidor cerró (timeout 2 s)


class TestAutenticacion(SistemaBase):
    token = "secreto"

    def test_token_correcto_e_incorrecto(self):
        self.assertTrue(self.cliente.enviar("factorial", 5)["ok"])
        sin_token = Cliente("127.0.0.1", self.servidor.puerto)
        r = sin_token.enviar("factorial", 5)
        self.assertFalse(r["ok"])
        self.assertIn("no autorizado", r["error"])
        self.assertFalse(Cliente("127.0.0.1", self.servidor.puerto, token="otro").estado()["ok"])


class TestSaturacion(unittest.TestCase):
    def test_cola_llena_rechaza_tareas(self):
        tmp = tempfile.mkdtemp()
        worker = Worker("w1", puerto=0, hilos=1)
        worker.iniciar()
        servidor = Servidor([("127.0.0.1", worker.puerto)], puerto=0, db=Path(tmp) / "t.db",
                            despachadores=1, capacidad_cola=1)
        servidor.iniciar()
        cliente = Cliente("127.0.0.1", servidor.puerto, timeout=10)
        try:
            with ThreadPoolExecutor(max_workers=6) as pool:
                respuestas = list(pool.map(lambda _: cliente.enviar("dormir", 0.5), range(6)))
            errores = [r for r in respuestas if not r["ok"]]
            self.assertTrue(errores)
            self.assertTrue(all("saturado" in r["error"] for r in errores))
            self.assertTrue(any(r["ok"] for r in respuestas))
        finally:
            servidor.detener()
            worker.detener()
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
