import sys
import json
import phoenixdb
import phoenixdb.cursor

from kafka import KafkaConsumer
from metric_list import *

# ./collector -a localhost:8080 -t COUNTERS_DB -q COUNTERS/Ethernet*/Queues  -client_types gnmi -insecure
# ./collector -a localhost:8080 -t COUNTERS_DB -q COUNTERS/Ethernet*  -client_types gnmi -insecure
# ./collector -a localhost:8080 -t APPL_DB -q PORT_TABLE,LLDP_ENTRY_TABLE,  -client_types gnmi -insecure

database_url='http://localhost:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)
cursor = conn.cursor()
print "db connection is established"
def create_db():
    cursor.execute("DROP TABLE IF EXISTS PORT_CONFIG")
    cursor.execute("CREATE TABLE PORT_CONFIG (port_name VARCHAR PRIMARY KEY, target_addr VARCHAR, \
                  lanes VARCHAR, admin_status VARCHAR, \
                  mtu VARCHAR, alias VARCHAR, oper_status VARCHAR, speed VARCHAR)")
    metric_str = ""
    for k in SAI_PORT_METRICS.keys():
        metric_str = metric_str +", " + k + " VARCHAR"
    create_port_stat_sql = "CREATE TABLE PORT_STAT (port_name VARCHAR PRIMARY KEY, target_addr VARCHAR "+ metric_str +" )"
    cursor.execute("DROP TABLE IF EXISTS PORT_STAT")
    cursor.execute(create_port_stat_sql)
    metric_str = ""
    for k in SAI_QUEUE_METRICS.keys():
        metric_str = metric_str +", " + k + " VARCHAR"
    create_queue_stat_sql = "CREATE TABLE QUEUE_STAT (queue_name VARCHAR PRIMARY KEY, target_addr VARCHAR "+ metric_str +" )"
    cursor.execute("DROP TABLE IF EXISTS QUEUE_STAT")
    cursor.execute(create_queue_stat_sql)
    cursor.execute("SELECT * FROM PORT_CONFIG")
    cursor.execute("SELECT * FROM PORT_STAT")
    cursor.execute("SELECT * FROM QUEUE_STAT")
    return  cursor.fetchall()
    #print create_port_stat_sql

bootstrap_servers = sys.argv[1] #"10.1.190.11:9093"
topic = sys.argv[2]


ret = create_db()
print "creating tables is done"
consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_servers, auto_offset_reset = 'latest')
print "Kafka Conumser configuration is done topic (%s)"  %(topic)

if consumer:
    for msg in consumer:
        target_addr = msg.key
        #print msg.value
        msg_value = json.loads(msg.value)
        for k,v in msg_value.items():
            if k == 'COUNTERS':
                Ethernet_astr = v['Ethernet*']
                E_key = Ethernet_astr.keys()
                if "Queues" in E_key:
                    Queue_list = Ethernet_astr['Queues']
                    for q, stat in Queue_list.items():
                        queue_name = str(q)
                        sql = "UPSERT INTO QUEUE_STAT VALUES  ( ? , ? "
                        for kk in stat.keys():
                            sql += ", ?"
                        sql += ")"

                        values_tuple = (queue_name, target_addr)
                        for vv in stat.values():
                            value = str(vv)
                            values_tuple += tuple(value)
                        print values_tuple
                        cursor.execute(sql, (values_tuple))
                else:
                    for port, stat in Ethernet_astr.items():
                        port_name = str(port)
                        sql = "UPSERT INTO PORT_STAT VALUES  ( ? , ?"
                        for kk in stat.keys():
                            sql += ", ?"
                        sql += ")"
                        values_tuple = (port_name, target_addr)
                        for vv in stat.values():
                            value = str(vv)
                            values_tuple += tuple(value)
                        print values_tuple
                        cursor.execute(sql, (values_tuple))


            if k == 'PORT_TABLE':
                for kk, vv in v.items():
                    port_name = str(kk)
                    if "Ethernet" in port_name:
                        lanes = str(vv['lanes'])
                        admin_status = str(vv['admin_status'])
                        mtu = str(vv['mtu'])
                        alias = str(vv['alias'])
                        oper_status = str(vv['oper_status'])
                        speed = str(vv['speed'])
                        sql = "UPSERT INTO PORT_CONFIG VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                        print (port_name, target_addr, lanes, admin_status, mtu, alias, oper_status, speed)
                        cursor.execute(sql, (port_name, target_addr, lanes, admin_status, mtu, alias, oper_status, speed))

            if k == 'NEIGH_TABLE':
                print v

            if k == 'LLDP_ENTRY_TABLE':
                print v
