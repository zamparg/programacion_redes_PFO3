"""Cliente de consola: envía tareas al servidor y muestra los resultados.

Ejemplos:
    python cliente.py registrar --usuario ana --contraseña clave
    python cliente.py enviar fibonacci 30 --usuario ana --contraseña clave
    python cliente.py enviar sumar "[1, 2, 3.5]" --usuario ana --contraseña clave
    python cliente.py consultar 1 --usuario ana --contraseña clave
    python cliente.py estado
    python cliente.py demo --tareas 12 --usuario ana --contraseña clave
    python cliente.py            (menú interactivo: registro o inicio de sesión)

Si falta --usuario o --contraseña, el cliente los pide por consola.
"""

import argparse
import getpass
import json
import os
import random
import threading
import time

import protocolo

HOST = "127.0.0.1"
PUERTO = 5050


class Cliente:
    def __init__(
        self,
        host: str = HOST,
        puerto: int = PUERTO,
        token: str | None = None,
        timeout: float = 130,
        usuario: str | None = None,
        contraseña: str | None = None,
    ):
        self.host = host
        self.puerto = puerto
        self.token = token
        self.timeout = timeout
        self.usuario = usuario
        self.contraseña = contraseña

    def _pedir(self, mensaje: dict) -> dict:
        if self.token is not None:
            mensaje = {**mensaje, "token": self.token}
        return protocolo.peticion(self.host, self.puerto, mensaje, self.timeout)

    def _pedir_autenticado(self, mensaje: dict) -> dict:
        return self._pedir({**mensaje, "usuario": self.usuario, "contraseña": self.contraseña})

    def registrar(self, usuario: str, contraseña: str) -> dict:
        return self._pedir({"accion": "registro", "usuario": usuario, "contraseña": contraseña})

    def login(self, usuario: str, contraseña: str) -> dict:
        return self._pedir({"accion": "login", "usuario": usuario, "contraseña": contraseña})

    def enviar(self, tipo: str, datos=None) -> dict:
        return self._pedir_autenticado({"accion": "enviar", "tipo": tipo, "datos": datos})

    def consultar(self, id_tarea: int) -> dict:
        return self._pedir_autenticado({"accion": "consultar", "id": id_tarea})

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


def mostrar_mensaje(respuesta: dict) -> None:
    if respuesta.get("ok"):
        print(respuesta.get("mensaje"))
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


def autenticarse(cliente: Cliente) -> bool:
    """Pregunta si registrarse o iniciar sesión; deja las credenciales cargadas en el cliente."""
    print("\nSistema distribuido de tareas")
    print("1. Registrarse")
    print("2. Iniciar sesión")
    opcion = input("Selecciona una opción: ").strip()
    if opcion not in {"1", "2"}:
        print("Opción no válida.")
        return False

    usuario = input("Usuario: ").strip()
    contraseña = getpass.getpass("Contraseña: ")
    if opcion == "1":
        respuesta = cliente.registrar(usuario, contraseña)
        mostrar_mensaje(respuesta)
        if not respuesta.get("ok"):
            return False

    respuesta = cliente.login(usuario, contraseña)
    if not respuesta.get("ok"):
        mostrar_mensaje(respuesta)
        return False
    cliente.usuario, cliente.contraseña = respuesta["usuario"], contraseña
    print(f"¡Bienvenido/a, {respuesta['usuario']}! Has iniciado sesión correctamente en el sistema de tareas.")
    return True


def menu(cliente: Cliente) -> None:
    if not autenticarse(cliente):
        return
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


def pedir_credenciales(args: argparse.Namespace, cliente: Cliente) -> None:
    cliente.usuario = args.usuario or input("Usuario: ").strip()
    cliente.contraseña = args.contraseña if args.contraseña is not None else getpass.getpass("Contraseña: ")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente del sistema distribuido (PFO 3)")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--puerto", type=int, default=PUERTO)
    parser.add_argument("--token", default=os.environ.get("PFO3_TOKEN"))

    credenciales = argparse.ArgumentParser(add_help=False)
    credenciales.add_argument("--usuario", help="usuario (si se omite, se pide)")
    credenciales.add_argument("--contraseña", help="contraseña (si se omite, se pide)")
    sub = parser.add_subparsers(dest="comando")

    sub.add_parser("registrar", parents=[credenciales], help="crea un usuario nuevo")
    sub.add_parser("login", parents=[credenciales], help="comprueba usuario y contraseña")
    p_enviar = sub.add_parser("enviar", parents=[credenciales], help="envía una tarea y espera el resultado")
    p_enviar.add_argument("tipo")
    p_enviar.add_argument("datos", nargs="?")
    p_consultar = sub.add_parser("consultar", parents=[credenciales], help="consulta una tarea propia por id")
    p_consultar.add_argument("id", type=int)
    sub.add_parser("estado", help="estado de la cola y de los workers")
    sub.add_parser("tipos", help="lista los tipos de tarea disponibles")
    p_demo = sub.add_parser("demo", parents=[credenciales], help="envía varias tareas concurrentes")
    p_demo.add_argument("--tareas", type=int, default=12)

    args = parser.parse_args()
    cliente = Cliente(args.host, args.puerto, args.token)
    try:
        if args.comando in {"registrar", "login"}:
            pedir_credenciales(args, cliente)
            if args.comando == "registrar":
                mostrar_mensaje(cliente.registrar(cliente.usuario, cliente.contraseña))
            else:
                mostrar_mensaje(cliente.login(cliente.usuario, cliente.contraseña))
        elif args.comando in {"enviar", "consultar", "demo"}:
            pedir_credenciales(args, cliente)
            if args.comando == "enviar":
                mostrar_respuesta(cliente.enviar(args.tipo, interpretar_datos(args.datos)))
            elif args.comando == "consultar":
                print(json.dumps(cliente.consultar(args.id), ensure_ascii=False, indent=2))
            else:
                demo(cliente, args.tareas)
        elif args.comando == "estado":
            mostrar_estado(cliente.estado())
        elif args.comando == "tipos":
            print(", ".join(cliente.tipos()))
        else:
            menu(cliente)
    except (OSError, protocolo.ProtocoloError) as error:
        print(f"No se pudo comunicar con el servidor ({args.host}:{args.puerto}): {error}")


if __name__ == "__main__":
    main()
