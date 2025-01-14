#-------------------------------------------------------------------------------------------------------------------
# BIG DATA PROCESSING ASSIGNMENT 2
# ABHINAV BOHRA 18CS30049
#-------------------------------------------------------------------------------------------------------------------
# Run the script using the command 
# python3.10 assignment-2-18CS30049.py <path to data two column graph file>
# python3.10 assignment-2-18CS30049.py input.txt
#-------------------------------------------------------------------------------------------------------------------

import sys
import random

#-------------------------------------------------------------------------------------------------------------------
# UTILITY FUNCTIONS FOR UNION-FIND
#-------------------------------------------------------------------------------------------------------------------
# Define a list disjoint_sets to store all independent disjoint sets.  
disjoint_sets = list()
    
# Define a class uf_set to represent a disjoint set in union-find algorithm.
class uf_set:
	# Initialize uf_set with parent_id and size attributes.
	def __init__(self, parent_id, size):
		self.parent_id = parent_id
		self.size = size

# Implement a function FindCommunity to find the set of an element i using path compression.
def FindCommunity(i):
	if disjoint_sets[i].parent_id != i:
		disjoint_sets[i].parent_id = FindCommunity(disjoint_sets[i].parent_id)
	return disjoint_sets[i].parent_id

# Implement a function MergeCommunity to do merge two sets x and y using union by size
def MergeCommunity(x, y):
	xroot = FindCommunity(x)
	yroot = FindCommunity(y)

	# Attach smaller tree under root of bigger tree (union by size)
	if disjoint_sets[xroot].size < disjoint_sets[yroot].size:
		disjoint_sets[xroot].parent_id = yroot
	elif disjoint_sets[xroot].size > disjoint_sets[yroot].size:
		disjoint_sets[yroot].parent_id = xroot
	# When sizes are same, then make one as root and increment its size by one
	else:
		disjoint_sets[yroot].parent_id = xroot
		disjoint_sets[xroot].size += 1
		
#-------------------------------------------------------------------------------------------------------------------
# DATA STRUCTURES (CLASSES) FOR STORING GRAPH-RELATED DATA
#-------------------------------------------------------------------------------------------------------------------

# A class to store undirected and unweighted graph
class Graph:
	def __init__(self, num_v, num_e, edges):
		self.V = num_v 			#Number of nodes
		self.E = num_e			#Number of edges
		self.edges = edges		#List of edges

# Implement a function createGraph to create a graph from a given list of edges.
def createGraph(edges):
	num_nodes = max([elem for tup in edges for elem in tup])
	num_edges = len(edges)
	graph = Graph(num_nodes, num_edges, edges)
	return graph

#-------------------------------------------------------------------------------------------------------------------
# Karger's randomized algorithm for finding the minimum cut
#-------------------------------------------------------------------------------------------------------------------
def kargerMincut(graph):
	#Get graph data
	V = graph.V
	E = graph.E
	edges = graph.edges

	# Create V singleton disjoint_sets
	for v in range(V+1):
		disjoint_sets.append(uf_set(v, 0))
		
	# Initially there are V nodes in contracted graph
	nodes = V

	# Keep contracting nodes until there are 2 nodes.
	while nodes > 2:
		# Select an edge randomly
		re = random.randint(0, E-1)
		
		# Find community of start and end nodes of the current edge
		set1 = FindCommunity(edges[re][0])
		set2 = FindCommunity(edges[re][1])

		# Contract the edge i.e. combine the nodes of edge into one node)
		if set1 != set2:			
			nodes -= 1 					# Reduce the node count			
			MergeCommunity(set1, set2) 	# Merge both sets
		else:			
			continue #both the nodes of current edge belong to same subset, then no need to consider the edge

	# Count the number of edges (mincut value) between final two components
	mincut = 0
	for i in range(E):
		set1 = FindCommunity(edges[i][0])
		set2 = FindCommunity(edges[i][1])
		if set1 != set2:
			mincut += 1

	return mincut

#-------------------------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#-------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
	# Parsing Command Line Arguments
	input_path = sys.argv[1]
	# Parsing Input two column graph file
	f = open(input_path, "r")
	edges = [tuple(map(int, line.split())) for line in f]
	# Creating graph
	graph = createGraph(edges)
	# Finding communities using Karger’s mincut algorithm
	mincut = kargerMincut(graph)
	# Printing results
	print(mincut)
	x = FindCommunity(1)
	for node in range(1, 1+graph.V):
		community = FindCommunity(node)
		if community == x:
			print(node, 1)
		else:
			print(node, 2)