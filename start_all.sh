#!/bin/bash
pkill -f "./node"
pkill -f "./router"
sleep 2

rm -rf /tmp/shardkv
mkdir -p /tmp/shardkv

ALL_RAFT_A="127.0.0.1:7000,127.0.0.1:7100,127.0.0.1:7200"
ALL_IDS_A="A1,A2,A3"
ALL_HTTP_A="127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120"
mkdir -p /tmp/shardkv/A1 /tmp/shardkv/A2 /tmp/shardkv/A3

./node --node-id A1 --shard-id A --raft-addr 127.0.0.1:7000 --http-addr 127.0.0.1:8100 --data-dir /tmp/shardkv/A1 --bootstrap --peers "$ALL_RAFT_A" --peer-ids "$ALL_IDS_A" --peers-http "$ALL_HTTP_A" > /tmp/shardkv/A1.log 2>&1 &
./node --node-id A2 --shard-id A --raft-addr 127.0.0.1:7100 --http-addr 127.0.0.1:8110 --data-dir /tmp/shardkv/A2 --peers "$ALL_RAFT_A" --peer-ids "$ALL_IDS_A" --peers-http "$ALL_HTTP_A" > /tmp/shardkv/A2.log 2>&1 &
./node --node-id A3 --shard-id A --raft-addr 127.0.0.1:7200 --http-addr 127.0.0.1:8120 --data-dir /tmp/shardkv/A3 --peers "$ALL_RAFT_A" --peer-ids "$ALL_IDS_A" --peers-http "$ALL_HTTP_A" > /tmp/shardkv/A3.log 2>&1 &

ALL_RAFT_B="127.0.0.1:7300,127.0.0.1:7400,127.0.0.1:7500"
ALL_IDS_B="B1,B2,B3"
ALL_HTTP_B="127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150"
mkdir -p /tmp/shardkv/B1 /tmp/shardkv/B2 /tmp/shardkv/B3

./node --node-id B1 --shard-id B --raft-addr 127.0.0.1:7300 --http-addr 127.0.0.1:8130 --data-dir /tmp/shardkv/B1 --bootstrap --peers "$ALL_RAFT_B" --peer-ids "$ALL_IDS_B" --peers-http "$ALL_HTTP_B" > /tmp/shardkv/B1.log 2>&1 &
./node --node-id B2 --shard-id B --raft-addr 127.0.0.1:7400 --http-addr 127.0.0.1:8140 --data-dir /tmp/shardkv/B2 --peers "$ALL_RAFT_B" --peer-ids "$ALL_IDS_B" --peers-http "$ALL_HTTP_B" > /tmp/shardkv/B2.log 2>&1 &
./node --node-id B3 --shard-id B --raft-addr 127.0.0.1:7500 --http-addr 127.0.0.1:8150 --data-dir /tmp/shardkv/B3 --peers "$ALL_RAFT_B" --peer-ids "$ALL_IDS_B" --peers-http "$ALL_HTTP_B" > /tmp/shardkv/B3.log 2>&1 &

sleep 5

./router --http-addr 127.0.0.1:9000 \
  --shards "A:127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120;B:127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150" \
  > /tmp/shardkv/router.log 2>&1 &

wait
