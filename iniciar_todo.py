"""Levanta el sistema completo en una sola terminal: N workers + el servidor.

    python iniciar_todo.py              # 3 workers (puertos 6001-6003) + servidor en 5050
    python iniciar_todo.py --workers 5

Ctrl+C detiene todos los procesos.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

AQUI = Path(__file__).parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Inicia workers y servidor del PFO 3")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--puerto-base", type=int, default=6001)
    parser.add_argument("--puerto", type=int, default=5050)
    parser.add_argument("--hilos", type=int, default=4, help="hilos por worker")
    args = parser.parse_args()

    procesos = []
    try:
        direcciones = []
        for i in range(args.workers):
            puerto = args.puerto_base + i
            direcciones.append(f"127.0.0.1:{puerto}")
            procesos.append(subprocess.Popen([
                sys.executable, str(AQUI / "worker.py"),
                "--nombre", f"worker-{i + 1}", "--puerto", str(puerto), "--hilos", str(args.hilos),
            ]))
        time.sleep(0.5)  # que los workers ya estén escuchando
        procesos.append(subprocess.Popen([
            sys.executable, str(AQUI / "servidor.py"),
            "--puerto", str(args.puerto), "--workers", ",".join(direcciones),
        ]))
        print(f"\nSistema en marcha: servidor en 127.0.0.1:{args.puerto}, workers {direcciones}")
        print("Ctrl+C para detener.\n")
        while all(p.poll() is None for p in procesos):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procesos:
            if p.poll() is None:
                p.terminate()
        for p in procesos:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
