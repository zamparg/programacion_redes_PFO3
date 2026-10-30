"""Cliente de consola: envía tareas al servidor y muestra los resultados.

Ejemplos:
    python cliente.py enviar fibonacci 30
    python cliente.py enviar sumar "[1, 2, 3.5]"
    python cliente.py enviar invertir "hola mundo"
    python cliente.py consultar 1
    python cliente.py estado
    python cliente.py demo --tareas 12
    python cliente.py            (menú interactivo)
"""

import argparse
import json
import os
import random
import threading
import time

import protocolo

HOST = "127.0.0.1"
PUERTO = 5050


class Cliente:
    def __init__(self, host: str = HOST, puerto: int = PUERTO, token: str | None = None, timeout: float = 130):
        self.host = host
        self.puerto = puerto
        self.token = token
        self.timeout = timeout

    def _pedir(self, mensaje: dict) -> dict:
        if self.token is not None:
            mensaje = {**mensaje, "token": self.token}
        return protocolo.peticion(self.host, self.puerto, mensaje, self.timeout)

    def enviar(self, tipo: str, datos=None) -> dict:
        return self._pedir({"accion": "enviar", "tipo": tipo, "datos": datos})

    def consultar(self, id_tarea: int) -> dict:
        return self._pedir({"accion": "consultar", "id": id_tarea})

    def estado(self) -> dict:
        return self._pedir({"accion": "estado"})

    def tipos(self) -> list[str]:
        return self._pedir({"accion": "tipos"}).get("tipos", [])


def interpretar_datos(texto: str | None):
    """'30' -> 30, '[1,2]' -> [1, 2]; cualquier otro texto queda como cadena."""
    if texto is None:
        return None
    try:
        return json.loads(texto)
    except ValueError:
        return texto


def mostrar_respuesta(respuesta: dict) -> None:
    if respuesta.get("ok"):
        resultado = json.dumps(respuesta.get("resultado"), ensure_ascii=False)
        if len(resultado) > 200:
            resultado = resultado[:200] + f"... ({len(resultado)} caracteres)"
        print(f"[tarea {respuesta.get('id')}] resultado: {resultado}")
        print(f"            worker: {respuesta.get('worker')}  tiempo: {respuesta.get('ms')} ms")
    else:
        print(f"[error] {respuesta.get('error')}")


def mostrar_estado(estado: dict) -> None:
    if not estado.get("ok"):
        mostrar_respuesta(estado)
        return
    print(f"Tareas recibidas: {estado['recibidas']}  en cola: {estado['en_cola']}")
    for w in estado["workers"]:
        situacion = "activo" if w["activo"] else "CAÍDO"
        print(f"  - {w['nombre']:<18} {situacion:<7} en curso: {w['en_curso']}  completadas: {w['completadas']}")


def demo(cliente: Cliente, cantidad: int) -> None:
    """Envía varias tareas a la vez desde hilos distintos (clientes concurrentes)."""
    pedidos = [
        ("fibonacci", lambda: random.randint(500, 5000)),
        ("factorial", lambda: random.randint(50, 500)),
        ("es_primo", lambda: random.randint(10**6, 10**9)),
        ("contar_primos", lambda: random.randint(10**5, 10**6)),
        ("hash_sha256", lambda: f"mensaje-{random.randint(1, 9999)}"),
        ("dormir", lambda: 0.5),
    ]
    trabajos = [(tipo, generar()) for tipo, generar in (random.choice(pedidos) for _ in range(cantidad))]
    resultados: list = [None] * len(trabajos)

    def ejecutar(i: int, tipo: str, datos) -> None:
        try:
            resultados[i] = cliente.enviar(tipo, datos)
        except OSError as error:
            resultados[i] = {"ok": False, "error": f"sin conexión: {error}"}

    inicio = time.perf_counter()
    hilos = [threading.Thread(target=ejecutar, args=(i, t, d)) for i, (t, d) in enumerate(trabajos)]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()
    total = time.perf_counter() - inicio

    por_worker: dict[str, int] = {}
    for (tipo, datos), respuesta in zip(trabajos, resultados):
        print(f"{tipo}({str(datos)[:30]})")
        mostrar_respuesta(respuesta)
        if respuesta.get("ok"):
            por_worker[respuesta["worker"]] = por_worker.get(respuesta["worker"], 0) + 1
    print(f"\n{cantidad} tareas en {total:.2f} s. Reparto por worker: {por_worker}")


def menu(cliente: Cliente) -> None:
    tipos = cliente.tipos()
    while True:
        print("\nSistema distribuido de tareas")
        print("1. Enviar tarea")
        print("2. Consultar tarea por id")
        print("3. Estado del sistema")
        print("4. Salir")
        opcion = input("Selecciona una opción: ").strip()
        if opcion == "1":
            print("Tipos disponibles:", ", ".join(tipos))
            tipo = input("Tipo: ").strip()
            datos = interpretar_datos(input("Datos (número, texto o JSON): "))
            mostrar_respuesta(cliente.enviar(tipo, datos))
        elif opcion == "2":
            texto = input("Id: ").strip()
            if not texto.isdigit():
                print("El id debe ser un número.")
                continue
            respuesta = cliente.consultar(int(texto))
            print(json.dumps(respuesta, ensure_ascii=False, indent=2))
        elif opcion == "3":
            mostrar_estado(cliente.estado())
        elif opcion == "4":
            return
        else:
            print("Opción no válida.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente del sistema distribuido (PFO 3)")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--puerto", type=int, default=PUERTO)
    parser.add_argument("--token", default=os.environ.get("PFO3_TOKEN"))
    sub = parser.add_subparsers(dest="comando")

    p_enviar = sub.add_parser("enviar", help="envía una tarea y espera el resultado")
    p_enviar.add_argument("tipo")
    p_enviar.add_argument("datos", nargs="?")
    p_consultar = sub.add_parser("consultar", help="consulta una tarea por id")
    p_consultar.add_argument("id", type=int)
    sub.add_parser("estado", help="estado de la cola y de los workers")
    sub.add_parser("tipos", help="lista los tipos de tarea disponibles")
    p_demo = sub.add_parser("demo", help="envía varias tareas concurrentes")
    p_demo.add_argument("--tareas", type=int, default=12)

    args = parser.parse_args()
    cliente = Cliente(args.host, args.puerto, args.token)
    try:
        if args.comando == "enviar":
            mostrar_respuesta(cliente.enviar(args.tipo, interpretar_datos(args.datos)))
        elif args.comando == "consultar":
            print(json.dumps(cliente.consultar(args.id), ensure_ascii=False, indent=2))
        elif args.comando == "estado":
            mostrar_estado(cliente.estado())
        elif args.comando == "tipos":
            print(", ".join(cliente.tipos()))
        elif args.comando == "demo":
            demo(cliente, args.tareas)
        else:
            menu(cliente)
    except (OSError, protocolo.ProtocoloError) as error:
        print(f"No se pudo comunicar con el servidor ({args.host}:{args.puerto}): {error}")


if __name__ == "__main__":
    main()
