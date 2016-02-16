import pyodbc as odbc
import networkx as nx
from sets import Set

cnxn = odbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=uniprotTest;Trusted_Connection=yes;')
cursor = cnxn.cursor()
cursor.execute("SELECT * FROM RDF")

nodes = Set()
edges = Set()
count = 0
while 1:
	row = cursor.fetchone()
	if not row:
		break
	subject = row.Subject
	predicate = row.Predicate
	object = row.Object
	
	
	nodes.add(subject)
	nodes.add(object)
	edges.add((subject, object))
	count += 1
	if count % 10000 == 0:
		print count
	
cnxn.close()

G = nx.DiGraph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)

print "Graph info:\n"
print len(G.nodes())
print len(G.edges())