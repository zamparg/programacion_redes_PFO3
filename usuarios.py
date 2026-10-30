"""Usuarios: registro e inicio de sesión con contraseñas hasheadas en SQLite.

Las contraseñas se guardan como hash scrypt con sal aleatoria, en el formato
"scrypt:N:r:p$sal$hash" que usa werkzeug. Solo usa la biblioteca estándar
(hashlib, hmac, secrets y sqlite3).
"""

import hashlib
import hmac
import secrets
import sqlite3
from contextlib import closing

ESQUEMA = """
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT NOT NULL
)
"""

N, R, P = 32768, 8, 1
MAXMEM = 64 * 1024 * 1024  # scrypt con N=32768 supera el límite por defecto de OpenSSL (32 MB)


def _derivar(contraseña: str, sal: str, n: int, r: int, p: int) -> str:
    clave = hashlib.scrypt(
        contraseña.encode("utf-8"), salt=sal.encode("utf-8"), n=n, r=r, p=p, maxmem=MAXMEM
    )
    return clave.hex()


def generar_hash(contraseña: str) -> str:
    sal = secrets.token_hex(16)
    return f"scrypt:{N}:{R}:{P}${sal}${_derivar(contraseña, sal, N, R, P)}"


def verificar_hash(guardado: str, contraseña: str) -> bool:
    try:
        metodo, sal, esperado = guardado.split("$")
        nombre, n, r, p = metodo.split(":")
        if nombre != "scrypt":
            return False
        calculado = _derivar(contraseña, sal, int(n), int(r), int(p))
    except ValueError:
        return False
    return hmac.compare_digest(calculado, esperado)


def registrar(db: str, usuario: str, contraseña: str) -> bool:
    """Devuelve False si el usuario ya existe (sin distinguir mayúsculas)."""
    try:
        with closing(sqlite3.connect(db)) as con:
            con.execute(
                "INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)",
                (usuario, generar_hash(contraseña)),
            )
            con.commit()
    except sqlite3.IntegrityError:
        return False
    return True


def verificar(db: str, usuario: str, contraseña: str) -> bool:
    with closing(sqlite3.connect(db)) as con:
        fila = con.execute(
            "SELECT password_hash FROM usuarios WHERE usuario = ?", (usuario,)
        ).fetchone()
    return fila is not None and verificar_hash(fila[0], contraseña)
