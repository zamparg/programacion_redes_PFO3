# Diagrama del sistema (Mermaid)

Se renderiza automáticamente en GitHub. Versión gráfica: [`diagrama.png`](diagrama.png) / [`diagrama.svg`](diagrama.svg).

```mermaid
flowchart TB
    subgraph CL["1. Clientes"]
        M["App móvil"]
        W["Cliente web"]
        P["Cliente Python<br/>cliente.py"]
    end

    LB["2. Balanceador de carga<br/>Nginx / HAProxy"]

    subgraph GW["3. Servidores / gateways (sin estado)"]
        S1["Servidor #1<br/>servidor.py<br/>pool de hilos + despachadores"]
        S2["Servidor #2<br/>(réplica)"]
    end

    Q[["4. Cola de mensajes<br/>RabbitMQ"]]

    subgraph WK["5. Workers"]
        W1["Worker 1<br/>pool de hilos"]
        W2["Worker 2<br/>pool de hilos"]
        W3["Worker 3<br/>pool de hilos"]
    end

    subgraph ST["6. Almacenamiento distribuido"]
        PG[("PostgreSQL<br/>primario + réplicas")]
        S3[("S3<br/>objetos")]
    end

    M -->|HTTPS| LB
    W -->|HTTPS| LB
    P -->|TCP + JSON| LB
    LB --> S1
    LB --> S2
    S1 -->|publica tarea| Q
    S2 -->|publica tarea| Q
    Q -->|consume| W1
    Q -->|consume| W2
    Q -->|consume| W3
    S1 -.->|registra / consulta| PG
    S2 -.->|registra / consulta| PG
    W1 --> PG
    W2 --> PG
    W3 --> S3
```

Implementado en el repositorio: `cliente.py`, `servidor.py` (incluye la cola como `queue.Queue` y el balanceo hacia los workers), `worker.py` y SQLite en lugar de PostgreSQL. Balanceador, RabbitMQ, réplicas y S3 son componentes de producción del diseño.
