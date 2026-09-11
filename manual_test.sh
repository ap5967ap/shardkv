#!/bin/bash
set -e

ROUTER="http://127.0.0.1:9000"

echo "Writing keys to range..."
curl -s -X PUT -H "Content-Type: application/json" -d '{"value": "val1"}' $ROUTER/kv/mykey-1 > /dev/null
curl -s -X PUT -H "Content-Type: application/json" -d '{"value": "val2"}' $ROUTER/kv/mykey-2 > /dev/null

echo "Keys on Shard A (before migration):"
curl -s "http://127.0.0.1:8100/admin/scan?start=mykey-&end=mykey-z" | grep -o mykey-[12] || echo "None"

echo "Keys on Shard B (before migration):"
curl -s "http://127.0.0.1:8130/admin/scan?start=mykey-&end=mykey-z" | grep -o mykey-[12] || echo "None"

echo "Triggering migration from A to B..."
curl -s -X POST -H "Content-Type: application/json" -d '{
  "from_shard": "A",
  "to_shard": "B",
  "key_range": {
    "start": "mykey-",
    "end": "mykey-z"
  }
}' $ROUTER/admin/migrate

echo -e "\nKeys on Shard A (after migration):"
curl -s "http://127.0.0.1:8100/admin/scan?start=mykey-&end=mykey-z" | grep -o mykey-[12] || echo "None"

echo "Keys on Shard B (after migration):"
curl -s "http://127.0.0.1:8130/admin/scan?start=mykey-&end=mykey-z" | grep -o mykey-[12] || echo "None"

echo "Reading from router (should hit Shard B):"
curl -s $ROUTER/kv/mykey-1
echo
