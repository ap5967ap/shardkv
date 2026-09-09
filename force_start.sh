#!/bin/bash
pkill -f node || true
pkill -f router || true
rm -rf /tmp/shardkv
mkdir -p /tmp/shardkv/{A1,A2,A3,B1,B2,B3,C1,C2,C3}
cd /home/ap/resume/kv

echo "Starting Nodes..."

# A
PA="127.0.0.1:7000,127.0.0.1:7100,127.0.0.1:7200"; IA="A1,A2,A3"; HA="127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120"
nohup ./node --node-id A1 --shard-id A --raft-addr 127.0.0.1:7000 --http-addr 127.0.0.1:8100 --data-dir /tmp/shardkv/A1 --bootstrap --peers "$PA" --peer-ids "$IA" --peers-http "$HA" > /tmp/shardkv/A1/node.log 2>&1 &
echo $! > /tmp/shardkv/A1.pid
nohup ./node --node-id A2 --shard-id A --raft-addr 127.0.0.1:7100 --http-addr 127.0.0.1:8110 --data-dir /tmp/shardkv/A2 --peers "$PA" --peer-ids "$IA" --peers-http "$HA" > /tmp/shardkv/A2/node.log 2>&1 &
echo $! > /tmp/shardkv/A2.pid
nohup ./node --node-id A3 --shard-id A --raft-addr 127.0.0.1:7200 --http-addr 127.0.0.1:8120 --data-dir /tmp/shardkv/A3 --peers "$PA" --peer-ids "$IA" --peers-http "$HA" > /tmp/shardkv/A3/node.log 2>&1 &
echo $! > /tmp/shardkv/A3.pid

# B
PB="127.0.0.1:7300,127.0.0.1:7400,127.0.0.1:7500"; IB="B1,B2,B3"; HB="127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150"
nohup ./node --node-id B1 --shard-id B --raft-addr 127.0.0.1:7300 --http-addr 127.0.0.1:8130 --data-dir /tmp/shardkv/B1 --bootstrap --peers "$PB" --peer-ids "$IB" --peers-http "$HB" > /tmp/shardkv/B1/node.log 2>&1 &
echo $! > /tmp/shardkv/B1.pid
nohup ./node --node-id B2 --shard-id B --raft-addr 127.0.0.1:7400 --http-addr 127.0.0.1:8140 --data-dir /tmp/shardkv/B2 --peers "$PB" --peer-ids "$IB" --peers-http "$HB" > /tmp/shardkv/B2/node.log 2>&1 &
echo $! > /tmp/shardkv/B2.pid
nohup ./node --node-id B3 --shard-id B --raft-addr 127.0.0.1:7500 --http-addr 127.0.0.1:8150 --data-dir /tmp/shardkv/B3 --peers "$PB" --peer-ids "$IB" --peers-http "$HB" > /tmp/shardkv/B3/node.log 2>&1 &
echo $! > /tmp/shardkv/B3.pid

# C
PC="127.0.0.1:7600,127.0.0.1:7700,127.0.0.1:7800"; IC="C1,C2,C3"; HC="127.0.0.1:8160,127.0.0.1:8170,127.0.0.1:8180"
nohup ./node --node-id C1 --shard-id C --raft-addr 127.0.0.1:7600 --http-addr 127.0.0.1:8160 --data-dir /tmp/shardkv/C1 --bootstrap --peers "$PC" --peer-ids "$IC" --peers-http "$HC" > /tmp/shardkv/C1/node.log 2>&1 &
echo $! > /tmp/shardkv/C1.pid
nohup ./node --node-id C2 --shard-id C --raft-addr 127.0.0.1:7700 --http-addr 127.0.0.1:8170 --data-dir /tmp/shardkv/C2 --peers "$PC" --peer-ids "$IC" --peers-http "$HC" > /tmp/shardkv/C2/node.log 2>&1 &
echo $! > /tmp/shardkv/C2.pid
nohup ./node --node-id C3 --shard-id C --raft-addr 127.0.0.1:7800 --http-addr 127.0.0.1:8180 --data-dir /tmp/shardkv/C3 --peers "$PC" --peer-ids "$IC" --peers-http "$HC" > /tmp/shardkv/C3/node.log 2>&1 &
echo $! > /tmp/shardkv/C3.pid

echo "Starting Router..."
nohup ./router --http-addr "127.0.0.1:9000" --shards "A:127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120;B:127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150;C:127.0.0.1:8160,127.0.0.1:8170,127.0.0.1:8180" > /tmp/shardkv/router.log 2>&1 &
echo $! > /tmp/shardkv/router.pid

disown -a
echo "All components running!"
