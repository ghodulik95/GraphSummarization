import pyodbc as odbc
import networkx as nx

cnxn = odbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=uniprotTest;Trusted_Connection=yes;')
cursor = cnxn.cursor()
cursor.execute("SELECT * FROM RDF")

G = nx.DiGraph()
while 1:
    row = cursor.fetchone()
    if not row:
        break
	subject = row.Subject
	predicate = row.Predicate
	object = row.Object
	
	if subject not in G.nodes():
		G.add_node(subject)
	if object not in G.nodes():
		G.add_node(object)
	if (subject, object) not in G.edges():
		G.add_edge(subject, object)
	
cnxn.close()