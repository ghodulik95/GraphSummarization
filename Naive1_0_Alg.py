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
		S.node[node]['cost'] = get_initial_cost(G,node)
		S.node[node]['contains'] = set([node])
		nodeToSupernode[node] = node
	print "Graph annotated"
	H = Q.PriorityQueue();
	
	count = 0
	for u in S.nodes():
		two_hop_neighbors = set()
		for neighbors1 in G.neighbors(u):
			for neighbors2 in G.neighbors(neighbors1):
				two_hop_neighbors.add(neighbors2)
		
		for v in two_hop_neighbors:
			suv = calcSUV(G, S, u, v, nodeToSupernode)
			if suv > 0:
				H.put(-suv, (u,v))
				print suv
		count += 1
		if count % 10000 == 0:
			print count
	i = 0
	while i in range(0,25) and len(H) > 0:		
		print H.get()
		i += 1

def get_initial_cost(G,node):
	return len(G.predecessors(node)) + len(G.successors(node))

#Expects original graph G, and nodes from S		
def calcSUV(G, S, u, v, nodeToSupernode):
	#beg = timeit.default_timer()

	costU = S.node[u]['cost']
	costV = S.node[v]['cost']
	numNodesW = len(S.node[u]['contains']) + len(S.node[v]['contains'])

	forward_neighbors = get_forward_neighbors(G, S, u, v, nodeToSupernode)
	inward_neighbors = get_inward_neighbors(G, S, u, v, nodeToSupernode)
	
	costW = 0
	#cur = timeit.default_timer()
	#print cur - beg
	#Calculate forward cost
	for fn in forward_neighbors:
		A_wn = 0
		for n in S.node[fn]['contains']:
			for uNode in S.node[u]['contains']:
				if G.has_edge(uNode, n):
					A_wn += 1
			for vNode in S.node[v]['contains']:
				if G.has_edge(vNode, n):
					A_wn += 1
		piWN = numNodesW * len(S.node[fn]['contains'])
		if piWN - A_wn + 1 < A_wn:
			costW += piWN - A_wn + 1
		else:
			costW += A_wn

	for nn in forward_neighbors:
		A_wn = 0
		for n in S.node[nn]['contains']:
			for uNode in S.node[u]['contains']:
				if G.has_edge(n, uNode):
					A_wn += 1
			for vNode in S.node[v]['contains']:
				if G.has_edge(n, uNode):
					A_wn += 1
		piWN = numNodesW * len(S.node[nn]['contains'])
		if piWN - A_wn + 1 < A_wn:
			costW += piWN - A_wn + 1
		else:
			costW += A_wn

	#Calculate inward cost
	#cur = timeit.default_timer()
	#print cur - beg
	#print str(costU)+" "+str(costV)+" "+str(costW)
	return float(costU + costV - costW)/float(costU + costV)

def get_forward_neighbors(G, S, u, v, nodeToSupernode):
	forward_neighbors = set()
	for uNode in S.node[u]['contains']:
		for neighbor in G.successors_iter(uNode):
			forward_neighbors.add(nodeToSupernode[neighbor])
	for vNode in S.node[v]['contains']:
		for neighbor in G.successors_iter(vNode):
			forward_neighbors.add(nodeToSupernode[neighbor])
	return forward_neighbors

def get_inward_neighbors(G, S, u, v, nodeToSupernode):
	forward_neighbors = set()
	for uNode in S.node[u]['contains']:
		for neighbor in G.predecessors_iter(uNode):
			forward_neighbors.add(nodeToSupernode[neighbor])
	for vNode in S.node[v]['contains']:
		for neighbor in G.predecessors_iter(vNode):
			forward_neighbors.add(nodeToSupernode[neighbor])
	return forward_neighbors

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
