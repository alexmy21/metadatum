# Import Multiplication from your library
import os
import time
import redis
from redis.commands.graph import Graph, Node, Edge

from metadatum.commands import Commands
from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc

utl.importConfig()
import config as cnf

lb="person"
properties_1 = {"name": "Sasha Mylnikov", "age": 76}
properties_2 = {"name": "Dima Mylnikov", "age": 47}

# r = redis.Redis(cnf.settings.redis.host, cnf.settings.redis.port, db=0)

pool = redis.ConnectionPool(host = cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
r = redis.Redis(connection_pool = pool)

cmd = Commands()

time_start = time.perf_counter()

# list of type Node
nodes = []

# declare list of Edges
edges = []

# create nodes
sasha = cmd.createNode(lb, properties_1)
dima = cmd.createNode(lb, properties_2)

# add nodes to the list of nodes
nodes.append(sasha)
nodes.append(dima)

edges.append(cmd.createEdge(sasha, "father", dima, {}))
edges.append(cmd.createEdge(dima, "son", sasha, {}))

graph = r.graph('meta')

cmd.mergeNodes(graph, nodes)

# cmd.mergeEdges(graph, edges)

cmd.commitGraph(graph)

print(graph.name)

dir = cnf.settings.dot_meta
idx_reg_file = os.path.join(dir, cnf.settings.indices.idx_reg_file)
# print(idx_reg_file)

reg, idx, sha_id = cmd.createRegistryIndex(r, idx_reg_file, 'main_core')

dir_core = os.path.join(dir, cnf.settings.indices.dir_core)

# list all files with ext in directory
file_list = utl.listAllFiles(dir_core, '.yaml')

# print(file_list)

for schema_file in file_list:
    print(schema_file)
    reg, idx, sha_id = cmd.createUserIndex(r, reg, schema_file, 'main_core')
    print(cmd.parseDocument(r, 'idx_reg:' + sha_id, schema_file))

time_end = time.perf_counter()
execution_time = time_end - time_start
print('Execution time: ', execution_time)