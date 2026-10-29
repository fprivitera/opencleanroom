#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Start or stop a Spark Connect server in Docker for tests and CI (ADR 0007).
#   scripts/spark-connect.sh up [version]     prints: export OCR_SPARK_URL=sc://127.0.0.1:<port>
#   scripts/spark-connect.sh down [version]
# Versions: 4.1.3 (default) bundles Spark Connect; 3.5.x fetches the connect package from Maven at start.
set -euo pipefail
cmd=${1:?usage: spark-connect.sh up|down [version]}
version=${2:-4.1.3}
name="ocr-spark-${version//./-}"
case "$cmd" in
  up)
    extra=()
    case "$version" in
      3.*) extra=(--packages "org.apache.spark:spark-connect_2.12:${version}" --conf spark.jars.ivy=/tmp/.ivy2) ;;
    esac
    docker rm -f "$name" >/dev/null 2>&1 || true
    docker run -d --name "$name" -p 127.0.0.1::15002 -e SPARK_NO_DAEMONIZE=1 "apache/spark:${version}" \
      /opt/spark/sbin/start-connect-server.sh ${extra[@]+"${extra[@]}"} >/dev/null
    port=$(docker port "$name" 15002/tcp 2>/dev/null | head -1 | sed 's/.*://') || true
    for _ in $(seq 1 180); do
      if [ -n "$port" ] && docker logs "$name" 2>&1 | grep "Spark Connect server started" >/dev/null; then
        echo "export OCR_SPARK_URL=sc://127.0.0.1:${port}"
        exit 0
      fi
      if [ "$(docker inspect -f '{{.State.Running}}' "$name")" != "true" ]; then break; fi
      sleep 1
    done
    docker logs "$name" 2>&1 | tail -20 >&2
    exit 1
    ;;
  down)
    docker rm -f "$name" >/dev/null 2>&1 || true
    ;;
  *)
    echo "usage: spark-connect.sh up|down [version]" >&2
    exit 2
    ;;
esac
