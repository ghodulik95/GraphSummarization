from sets import Set
import pyodbc as odbc
import networkx as nx
import timeit
import heapq


def main():
    # getGraphSummary("uniprotTest")
    getGraphSummary("LUBMOld")


def initialization(dbname):
    g = getGraphFromRDFDB(dbname)
    print "Graph built"
    s = getGraphFromRDFDB(dbname, False)
    print "Graph copied"
    nodeToSupernode = {}
    for node in s.nodes():
        s.node[node]['cost'] = get_initial_cost(g, node)
        s.node[node]['contains'] = set([node])
        nodeToSupernode[node] = node
    print "Graph annotated"
    h = []

    count = 0
    for u in s.nodes():
        two_hop_neighbors = get_two_hop_neighbors(g,u)

        for v in two_hop_neighbors:
            suv = calcSUV(g, s, u, v, nodeToSupernode)
            if suv > 0:
                heapq.heappush(h, (-suv, (u, v)))
        count += 1
        if count % 10 == 0:
            print count
        if count % 200 == 0:
            break
    return g, s, h, nodeToSupernode

def get_two_hop_neighbors(g,u):
    two_hop_neighbors = set()
    for neighbors1 in g.successors(u):
        for neighbors2 in g.predecessors(neighbors1):
            if neighbors2 != u:
                two_hop_neighbors.add(neighbors2)
    for neighbors1 in g.predecessors(u):
        for neighbors2 in g.successors(neighbors1):
            if neighbors2 != u:
                two_hop_neighbors.add(neighbors2)
    return two_hop_neighbors

def getGraphSummary(dbname):
    g, s, h, nodeToSupernode = initialization(dbname)

    while len(h) > 0:
        p, (u, v) = h.pop()
        new_node_set = s[u]['contains'].union(s[v]['contains'])
        new_node = get_supernode_name(new_node_set)
        s.add_node(new_node)
        s[new_node]['contains'] = new_node_set
        nodeToSupernode[u] = new_node
        nodeToSupernode[v] = new_node
        s.remove_node(u)
        s.remove_node(v)
        print p
        #for x in get_two_hop_neighbors(g,u):

        #heapq.heapify(h)

def get_supernode_name(nodes):
    return str(sorted(list(nodes)))

def get_initial_cost(G, node):
    return len(G.predecessors(node)) + len(G.successors(node))


# Expects original graph G, and nodes from S
def calcSUV(G, S, u, v, nodeToSupernode):
    # beg = timeit.default_timer()

    costU = S.node[u]['cost']
    costV = S.node[v]['cost']
    numNodesW = len(S.node[u]['contains']) + len(S.node[v]['contains'])

    forward_neighbors = get_forward_neighbors(G, S, u, v, nodeToSupernode)
    inward_neighbors = get_inward_neighbors(G, S, u, v, nodeToSupernode)

    costW = 0
    # cur = timeit.default_timer()
    # print cur - beg
    # Calculate forward cost
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

    # Calculate inward cost
    # cur = timeit.default_timer()
    # print cur - beg
    # print str(costU)+" "+str(costV)+" "+str(costW)
    return float(costU + costV - costW) / float(costU + costV)


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


def getGraphFromRDFDB(dbname, includeEdges=True):
    cnxn = odbc.connect(r'Driver={SQL Server};Server=.\SQLEXPRESS;Database=' + dbname + r';Trusted_Connection=yes;')
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
    if includeEdges:
        G.add_edges_from(edges)
    return G


# print "Graph info:\n"
# print len(G.nodes())
# print len(G.edges())



if __name__ == "__main__":
    main()
