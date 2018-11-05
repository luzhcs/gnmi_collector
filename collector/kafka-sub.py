import sys
import json
import phoenixdb
import phoenixdb.cursor

from kafka import KafkaConsumer

# ./collector -a localhost:8080 -t COUNTERS_DB -q COUNTERS/Ethernet*/Queues,COUNTERS/Ethernet*  -client_types gnmi -insecure
# ./collector -a localhost:8080 -t APPL_DB -q PORT_TABLE,LLDP_ENTRY_TABLE,  -client_types gnmi -insecure


database_url='http://localhost:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()

def create_db():
    cursor.execute("DROP TABLE IF EXISTS PORT_CONFIG")
    cursor.execute("CREATE TABLE PORT_CONFIG (port_name VARCHAR PRIMARY KEY, target_addr VARCHAR \
                   lanes VARCHAR, \
                   admin_status VARCHAR, \
                   mtu VARCHAR, \
                   alias VARCHAR, \
                   oper_status VARCHAR, \
                   speed VARCHAR)")

bootstrap_servers = sys.argv[1] #"10.1.190.11:9093"
topic = sys.argv[2]
print "consume from topic: %s" %(topic)

create_db()
consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'latest')

for msg in consumer:
    target_addr = msg.key
    print msg.value
    msg_value = json.loads(msg.value)
    for k,v in msg_value.items():
        if k == 'COUNTERS':
            for kk, vv in v.items():
                port_name = kk
        if k == 'PORT_TABLE':
            for kk, vv in v.items():
                port_name = kk
                if "Ethernet" in port_name:
                    lanes = vv['lanes']
                    admin_status = vv['admin_status']
                    mtu = vv['mtu']
                    alias = vv['alias']
                    oper_status = vv['oper_status']
                    speed = vv['speed']
                    sql = "UPSERT INTO PORT VALUES_CONFIG (?, ?, ?, ?, ?, ?, ?, ?)"
                    print  (port_name, target_addr, lanes, admin_status, mtu, alias, oper_status, speed)
                    cursor.execute(sql, (port_name, target_addr, lanes, admin_status, mtu, alias, oper_status, speed))

        if k == 'NEIGH_TABLE':
            print v

        if k == 'LLDP_ENTRY_TABLE':
            print v
