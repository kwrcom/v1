#!/bin/bash
# wait_for_it.sh

host="$1"
port="$2"
shift 2
cmd="$@"

echo "Waiting for $host:$port..."

# Loop until we can connect to the host:port
while ! timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; do
  sleep 1
done

echo "$host:$port is available"
exec $cmd
