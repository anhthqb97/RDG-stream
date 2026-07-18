#!/bin/bash
set -euo pipefail
echo "==> Creating Kafka topics..."
bash /kafka/init-topics.sh
echo "==> Init complete."
