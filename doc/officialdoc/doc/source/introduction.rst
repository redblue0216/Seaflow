============
Introduction
============
Seaflow is an algorithm orchestration tool developed based on directed acyclic graphs. Its main functions include algorithm workflow design, runtime scheduling parallel automatic optimization, cache optimization, and support for heterogeneity. Its main technologies include metaprogramming technology, Ray computing engine, and Networkx graph theory technology.

**Main concepts**

	- DAGrunner: 
	DAG executor is an execution control for Seaflow runtime, which mainly manages the actual calculation and execution of directed acyclic graphs
	
	- Sequencerunner: 
	The sequence executor is an execution control for Seaflow runtime, which mainly manages the actual calculation and execution of sequence pipelines
	
	- Scheduler: 
	The scheduler is an orchestration control for Seaflow runtime, providing algorithm orchestration and directed acyclic graph related algorithms
	
	- Datacatalog: 
	The data directory is a storage control for Seaflow runtime, mainly providing data caching and data interaction methods
	
	- Hook_manager: 
	Mount management is the plugin management for Seaflow runtime, with the main function providing dynamic plugin functionality
	
	- Node: 
	Node is the basic unit of Seaflow runtime, which needs to be object-oriented encapsulated according to the objective function, supports parameter dependency, and needs to set input and output
	
	- Function: 
	Function is another basic unit of Seaflow runtime that does not require object-oriented encapsulation, does not support parameter dependency, and cannot use input

