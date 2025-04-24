# Cloud Computing Raft3D

A distributed 3D-printing cluster service built on the Raft consensus protocol. Raft3D allows you to manage printers, filaments, and print jobs across a fault-tolerant cluster of nodes.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Environment Setup](#environment-setup)
- [Running the Raft Nodes](#running-the-raft-nodes)
  - [Node 1 (Bootstrap)](#node-1-bootstrap)
  - [Node 2 (Join)](#node-2-join)
  - [Node 3 (Join)](#node-3-join)
- [Cluster Status](#cluster-status)
- [Using the HTTP API](#using-the-http-api)
  - [Printers](#printers)
    - [Create a Printer](#create-a-printer)
    - [List Printers](#list-printers)
  - [Filaments](#filaments)
    - [Create a Filament](#create-a-filament)
    - [List Filaments](#list-filaments)
  - [Print Jobs](#print-jobs)
    - [Create a Print Job](#create-a-print-job)
    - [List Print Jobs](#list-print-jobs)
    - [Filter Queued Jobs](#filter-queued-jobs)
    - [Get a Specific Job](#get-a-specific-job)
    - [Update Job Status](#update-job-status)
- [Testing Follower Redirect](#testing-follower-redirect)

---

## Prerequisites

- Go runtime (for building `raft3dapp`)
- [jq](https://stedolan.github.io/jq/) (for formatting JSON responses)
- A UNIX-like shell (Linux, macOS, or WSL)


## Project Structure

```text
├── raft3dapp       # Compiled Raft3D binary
├── data/
│   ├── node1/      # Persistent data for node1
│   ├── node2/      # Persistent data for node2
│   └── node3/      # Persistent data for node3
└── README.md       # This file
```

---

## Environment Setup

Before running each node, you can optionally clean up any old data:

```bash
rm -rf ./data/nodeX
```

Export the following environment variables (or pass equivalent flags):

| Variable               | Description                                 | Example                    |
|------------------------|---------------------------------------------|----------------------------|
| `RAFT3D_NODE_ID`       | Unique node identifier                      | `node1`, `node2`, `node3`  |
| `RAFT3D_RAFT_ADDR`     | Raft protocol listen address (host:port)    | `127.0.0.1:12001`          |
| `RAFT3D_HTTP_ADDR`     | HTTP API listen address (host:port)         | `127.0.0.1:8081`           |

---

## Running the Raft Nodes

### Node 1 (Bootstrap)

Bootstraps the cluster with all peers:

```bash
export RAFT3D_NODE_ID=node1
export RAFT3D_RAFT_ADDR="127.0.0.1:12001"
export RAFT3D_HTTP_ADDR="127.0.0.1:8081"

./raft3dapp \
  -bootstrap \
  -peer-ids=node1,node2,node3 \
  -peer-addrs=127.0.0.1:12001,127.0.0.1:12002,127.0.0.1:12003
```

### Node 2 (Join)

Joins the cluster via Node 1’s Raft address:

```bash
export RAFT3D_NODE_ID=node2
export RAFT3D_RAFT_ADDR="127.0.0.1:12002"
export RAFT3D_HTTP_ADDR="127.0.0.1:8082"

./raft3dapp -join-addr=127.0.0.1:12001
```

### Node 3 (Join)

Also joins via Node 1:

```bash
export RAFT3D_NODE_ID=node3
export RAFT3D_RAFT_ADDR="127.0.0.1:12003"
export RAFT3D_HTTP_ADDR="127.0.0.1:8083"

./raft3dapp -join-addr=127.0.0.1:12001
```

---

## Cluster Status

Check each node’s role and term:

```bash
curl http://127.0.0.1:8081/status | jq .
curl http://127.0.0.1:8082/status | jq .
curl http://127.0.0.1:8083/status | jq .
```

---

## Using the HTTP API

### Printers

#### Create a Printer

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"company": "Prusa", "model": "MK3S+"}' \
  http://127.0.0.1:8081/api/v1/printers
```

#### List Printers

```bash
curl http://127.0.0.1:8082/api/v1/printers | jq .
```

### Filaments

#### Create a Filament

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"type": "PETG", "color": "Orange", "total_weight_in_grams": 1000}' \
  http://127.0.0.1:8081/api/v1/filaments
```

Note the returned `id` (e.g., `filament-id-123`).

#### List Filaments

```bash
curl http://127.0.0.1:8083/api/v1/filaments | jq .
```

### Print Jobs

#### Create a Print Job

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"printer_id": "<printer-id>", "filament_id": "<filament-id-123>", "filepath": "/models/calibration_cube.gcode", "print_weight_in_grams": 8}' \
  http://127.0.0.1:8081/api/v1/print_jobs
```

Note the returned `id` (e.g., `job-id-abc`).

#### List Print Jobs

```bash
curl http://127.0.0.1:8081/api/v1/print_jobs | jq .
```

#### Filter Queued Jobs

```bash
curl "http://127.0.0.1:8082/api/v1/print_jobs?status=Queued" | jq .
```

#### Get a Specific Job

```bash
curl http://127.0.0.1:8083/api/v1/print_jobs/<job-id-abc> | jq .
```

#### Update Job Status

```bash
# To "Running"
curl -X PUT -H "Content-Type: application/json" \
  -d '{"status": "Running"}' \
  http://127.0.0.1:8081/api/v1/print_jobs/<job-id-abc>/status

# To "Done"
curl -X PUT -H "Content-Type: application/json" \
  -d '{"status": "Done"}' \
  http://127.0.0.1:8081/api/v1/print_jobs/<job-id-abc>/status
```

---

## Testing Follower Redirect

Verify that write requests on a follower are redirected to the leader:

```bash
curl -v -X POST -H "Content-Type: application/json" \
  -d '{"company": "Anycubic", "model": "Kobra"}' \
  http://127.0.0.1:8082/api/v1/printers
```

Look for `HTTP/1.1 307 Temporary Redirect` and a `Location:` header pointing to the leader (e.g., `:8081`).

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


