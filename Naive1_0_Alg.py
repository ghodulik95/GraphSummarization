from sets import Set
import Queue as Q
import pyodbc as odbc
import networkx as nx
import timeit

def main():
	#getGraphSummary("uniprotTest")
	getGraphSummary("LUBMOld")

def getGraphSummary(dbname):
	G = getGraphFromRDFDB(dbname)
	print "Graph built"
	S = G.copy()
	print "Graph copied"
	nodeToSupernode = {}
	for node in S.nodes():
		S.node[node]['cost'] = len(list(nx.all_neighbors(S,node)))
		S.node[node]['contains'] = set([node])
		nodeToSupernode[node] = node
	print "Graph annotated"
	H = Q.PriorityQueue();
	
	count = 0
	for u in S.nodes():
		two_hop_neighbors = Set()
		for neighbors1 in nx.all_neighbors(G,u):
			for neighbors2 in nx.all_neighbors(G,neighbors1):
				if neighbors2 != u:
					two_hop_neighbors.add(neighbors2) 
		
		for v in two_hop_neighbors:
			suv = calcSUV(G, S, u, v, nodeToSupernode)
			if suv > 0:
				H.put(-suv, (u,v))
				#print suv
		count += 1
		if count % 10 == 0:
			print count
	for _ in range(0,25):		
		print H.get()
		

#Expects original graph G, and nodes from S		
def calcSUV(G, S, u, v, nodeToSupernode):
	#beg = timeit.default_timer()
	neighbors = set()
	for uNode in S.node[u]['contains']:
		for neighbor in nx.all_neighbors(G,uNode):
			neighbors.add(nodeToSupernode[neighbor])
	for vNode in S.node[v]['contains']:
		for neighbor in nx.all_neighbors(G,vNode):
			neighbors.add(nodeToSupernode[neighbor])
	
	costU = S.node[u]['cost']
	costV = S.node[v]['cost']
	
	numNodesW = len(S.node[u]['contains']) + len(S.node[v]['contains'])
	costW = 0
	#cur = timeit.default_timer()
	#print cur - beg
	for ns in neighbors:
		A_wn = 0
		A_nw = 0
		for n in S.node[ns]['contains']:
			for uNode in S.node[u]['contains']:
				if G.has_edge(uNode, n):
					A_wn += 1
				if G.has_edge(n, uNode):
					A_nw += 1
			for vNode in S.node[v]['contains']:
				if G.has_edge(vNode, n):
					A_wn += 1
				if G.has_edge(n, vNode):
					A_nw += 1
		piWN = numNodesW * len(S.node[ns]['contains'])
		if piWN - A_wn + 1 < A_wn:
			costW += piWN - A_wn + 1
		else:
			costW += A_wn
		if piWN - A_nw + 1 < A_nw:
			costW += piWN - A_nw + 1
		else:
			costW += A_nw
	#cur = timeit.default_timer()
	#print cur - beg
	#print str(costU)+" "+str(costV)+" "+str(costW)
	return float(costU + costV - costW)/float(costU + costV)

def getGraphFromRDFDB(dbname):
	cnxn = odbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database='+dbname+r';Trusted_Connection=yes;')
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
		
	cnxn.close()

	G = nx.DiGraph()
	G.add_nodes_from(nodes)
	G.add_edges_from(edges)
	return G

	#print "Graph info:\n"
	#print len(G.nodes())
	#print len(G.edges())
	

	
if __name__ == "__main__":
	main()
