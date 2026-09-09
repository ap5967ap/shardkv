import subprocess
import time
import os
import signal

os.system("pkill -f 'node '")
os.system("pkill -f 'router '")
os.system("rm -rf /tmp/shardkv")
os.system("mkdir -p /tmp/shardkv/A1 /tmp/shardkv/A2 /tmp/shardkv/A3 /tmp/shardkv/B1 /tmp/shardkv/B2 /tmp/shardkv/B3 /tmp/shardkv/C1 /tmp/shardkv/C2 /tmp/shardkv/C3")

def spawn(cmd, log_file, pid_file):
    f = open(log_file, "w")
    p = subprocess.Popen(cmd.split(), stdout=f, stderr=f, start_new_session=True)
    with open(pid_file, "w") as pf:
        pf.write(str(p.pid))
    return p

print("Starting nodes...")
# Shard A
PA="127.0.0.1:7000,127.0.0.1:7100,127.0.0.1:7200"
IA="A1,A2,A3"
HA="127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120"
spawn(f"./node --node-id A1 --shard-id A --raft-addr 127.0.0.1:7000 --http-addr 127.0.0.1:8100 --data-dir /tmp/shardkv/A1 --bootstrap --peers {PA} --peer-ids {IA} --peers-http {HA}", "/tmp/shardkv/A1/node.log", "/tmp/shardkv/A1.pid")
spawn(f"./node --node-id A2 --shard-id A --raft-addr 127.0.0.1:7100 --http-addr 127.0.0.1:8110 --data-dir /tmp/shardkv/A2 --peers {PA} --peer-ids {IA} --peers-http {HA}", "/tmp/shardkv/A2/node.log", "/tmp/shardkv/A2.pid")
spawn(f"./node --node-id A3 --shard-id A --raft-addr 127.0.0.1:7200 --http-addr 127.0.0.1:8120 --data-dir /tmp/shardkv/A3 --peers {PA} --peer-ids {IA} --peers-http {HA}", "/tmp/shardkv/A3/node.log", "/tmp/shardkv/A3.pid")

# Shard B
PB="127.0.0.1:7300,127.0.0.1:7400,127.0.0.1:7500"
IB="B1,B2,B3"
HB="127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150"
spawn(f"./node --node-id B1 --shard-id B --raft-addr 127.0.0.1:7300 --http-addr 127.0.0.1:8130 --data-dir /tmp/shardkv/B1 --bootstrap --peers {PB} --peer-ids {IB} --peers-http {HB}", "/tmp/shardkv/B1/node.log", "/tmp/shardkv/B1.pid")
spawn(f"./node --node-id B2 --shard-id B --raft-addr 127.0.0.1:7400 --http-addr 127.0.0.1:8140 --data-dir /tmp/shardkv/B2 --peers {PB} --peer-ids {IB} --peers-http {HB}", "/tmp/shardkv/B2/node.log", "/tmp/shardkv/B2.pid")
spawn(f"./node --node-id B3 --shard-id B --raft-addr 127.0.0.1:7500 --http-addr 127.0.0.1:8150 --data-dir /tmp/shardkv/B3 --peers {PB} --peer-ids {IB} --peers-http {HB}", "/tmp/shardkv/B3/node.log", "/tmp/shardkv/B3.pid")

# Shard C
PC="127.0.0.1:7600,127.0.0.1:7700,127.0.0.1:7800"
IC="C1,C2,C3"
HC="127.0.0.1:8160,127.0.0.1:8170,127.0.0.1:8180"
spawn(f"./node --node-id C1 --shard-id C --raft-addr 127.0.0.1:7600 --http-addr 127.0.0.1:8160 --data-dir /tmp/shardkv/C1 --bootstrap --peers {PC} --peer-ids {IC} --peers-http {HC}", "/tmp/shardkv/C1/node.log", "/tmp/shardkv/C1.pid")
spawn(f"./node --node-id C2 --shard-id C --raft-addr 127.0.0.1:7700 --http-addr 127.0.0.1:8170 --data-dir /tmp/shardkv/C2 --peers {PC} --peer-ids {IC} --peers-http {HC}", "/tmp/shardkv/C2/node.log", "/tmp/shardkv/C2.pid")
spawn(f"./node --node-id C3 --shard-id C --raft-addr 127.0.0.1:7800 --http-addr 127.0.0.1:8180 --data-dir /tmp/shardkv/C3 --peers {PC} --peer-ids {IC} --peers-http {HC}", "/tmp/shardkv/C3/node.log", "/tmp/shardkv/C3.pid")

print("Starting router...")
spawn(f"./router --http-addr 127.0.0.1:9000 --shards A:127.0.0.1:8100,127.0.0.1:8110,127.0.0.1:8120;B:127.0.0.1:8130,127.0.0.1:8140,127.0.0.1:8150;C:127.0.0.1:8160,127.0.0.1:8170,127.0.0.1:8180", "/tmp/shardkv/router.log", "/tmp/shardkv/router.pid")

print("Cluster started! Background processes are running.")
