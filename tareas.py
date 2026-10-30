"""Catálogo de tareas que saben ejecutar los workers.

Cada función recibe el campo "datos" del pedido y devuelve un resultado
serializable en JSON. Si los datos no son válidos lanza ValueError, que el
worker informa como error de la tarea (no es un fallo del worker).
"""

import hashlib
import time


def _entero(datos, minimo: int, maximo: int) -> int:
    if isinstance(datos, bool):
        raise ValueError("se esperaba un número entero")
    try:
        n = int(datos)
    except (TypeError, ValueError):
        raise ValueError("se esperaba un número entero") from None
    if not minimo <= n <= maximo:
        raise ValueError(f"el número debe estar entre {minimo} y {maximo}")
    return n


def fibonacci(datos):
    n = _entero(datos, 0, 10_000)
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def factorial(datos):
    n = _entero(datos, 0, 1_000)
    resultado = 1
    for i in range(2, n + 1):
        resultado *= i
    return resultado


def es_primo(datos):
    n = _entero(datos, 0, 10**12)
    if n < 2:
        return {"n": n, "es_primo": False}
    if n % 2 == 0:
        return {"n": n, "es_primo": n == 2}
    divisor = 3
    while divisor * divisor <= n:
        if n % divisor == 0:
            return {"n": n, "es_primo": False, "divisor": divisor}
        divisor += 2
    return {"n": n, "es_primo": True}


def contar_primos(datos):
    n = _entero(datos, 0, 5_000_000)
    if n < 2:
        return {"hasta": n, "cantidad": 0}
    criba = bytearray([1]) * (n + 1)
    criba[0] = criba[1] = 0
    for i in range(2, int(n**0.5) + 1):
        if criba[i]:
            criba[i * i :: i] = bytearray(len(range(i * i, n + 1, i)))
    return {"hasta": n, "cantidad": sum(criba)}


def invertir(datos):
    return str(datos)[::-1]


def mayusculas(datos):
    return str(datos).upper()


def contar_palabras(datos):
    return len(str(datos).split())


def sumar(datos):
    if not isinstance(datos, list) or not datos or len(datos) > 100_000:
        raise ValueError("se esperaba una lista de 1 a 100000 números")
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) for x in datos):
        raise ValueError("la lista solo puede contener números")
    return sum(datos)


def hash_sha256(datos):
    return hashlib.sha256(str(datos).encode("utf-8")).hexdigest()


def dormir(datos):
    """Simula una tarea lenta (E/S, consulta remota) para probar concurrencia."""
    try:
        segundos = float(datos)
    except (TypeError, ValueError):
        raise ValueError("se esperaba una cantidad de segundos") from None
    if not 0 <= segundos <= 10:
        raise ValueError("los segundos deben estar entre 0 y 10")
    time.sleep(segundos)
    return f"dormí {segundos} s"


TAREAS = {
    "fibonacci": fibonacci,
    "factorial": factorial,
    "es_primo": es_primo,
    "contar_primos": contar_primos,
    "invertir": invertir,
    "mayusculas": mayusculas,
    "contar_palabras": contar_palabras,
    "sumar": sumar,
    "hash_sha256": hash_sha256,
    "dormir": dormir,
}
