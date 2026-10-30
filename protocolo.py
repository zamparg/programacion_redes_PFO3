"""Protocolo de mensajes compartido por cliente, servidor y workers.

Cada mensaje es un objeto JSON en una sola línea terminada en "\\n" (JSON
Lines sobre TCP). TCP es un flujo de bytes sin límites de mensaje, por eso
se usa el salto de línea como delimitador.
"""

import json
import socket

MAX_LINEA = 1_000_000  # bytes; evita que un cliente agote la memoria


class ProtocoloError(ValueError):
    """Mensaje mal formado; la conexión sigue siendo utilizable."""


class LineaDemasiadoLarga(ProtocoloError):
    """El flujo quedó desincronizado; hay que cerrar la conexión."""


def enviar(sock: socket.socket, mensaje: dict) -> None:
    datos = json.dumps(mensaje, ensure_ascii=False).encode("utf-8") + b"\n"
    sock.sendall(datos)


def recibir(archivo) -> dict | None:
    """Lee un mensaje de un archivo creado con sock.makefile('rb').

    Devuelve None si el otro extremo cerró la conexión.
    """
    linea = archivo.readline(MAX_LINEA + 1)
    if not linea:
        return None
    if len(linea) > MAX_LINEA or not linea.endswith(b"\n"):
        raise LineaDemasiadoLarga("mensaje demasiado largo o incompleto")
    try:
        mensaje = json.loads(linea)
    except ValueError as error:
        raise ProtocoloError(f"JSON inválido: {error}") from None
    if not isinstance(mensaje, dict):
        raise ProtocoloError("el mensaje debe ser un objeto JSON")
    return mensaje


def peticion(host: str, puerto: int, mensaje: dict, timeout: float = 30) -> dict:
    """Abre una conexión, envía un mensaje y devuelve la respuesta."""
    with socket.create_connection((host, puerto), timeout=timeout) as sock:
        enviar(sock, mensaje)
        with sock.makefile("rb") as archivo:
            respuesta = recibir(archivo)
    if respuesta is None:
        raise ConnectionError("el otro extremo cerró la conexión sin responder")
    return respuesta
