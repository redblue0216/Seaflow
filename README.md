# ![seaflow_avatar](static/seaflow_avatar.png)
![shields_version](static/shields_version.svg)  ![shields_license](static/shields_license.svg)   ![shields_author](static/shields_author.svg)  ![shiedls_python](static/shields_python.svg)

## About seaflow

Seaflow is an algorithm orchestration tool developed based on directed acyclic graphs. Its main functions include algorithm workflow design, runtime scheduling parallel automatic optimization, cache optimization, and support for heterogeneity. Its main technologies include metaprogramming technology, Ray computing engine, and Networkx graph theory technology.


## Documentation

The documentationn for user guide  is at 

<https://seaflow-officialdoc.netlify.app>

The documentation for code comments is at

<https://seaflow-codedoc.netlify.app>


## Main Concepts

+ **DAGrunner:**
DAG executor is an execution control for Seaflow runtime, which mainly manages the actual calculation and execution of directed acyclic graphs.
+ **Sequencerunner:**
The sequence executor is an execution control for Seaflow runtime, which mainly manages the actual calculation and execution of sequence pipelines.
+ **Scheduler:**
The scheduler is an orchestration control for Seaflow runtime, providing algorithm orchestration and directed acyclic graph related algorithms.
+ **Datacatalog:**
The data directory is a storage control for Seaflow runtime, mainly providing data caching and data interaction methods.
+ **Hook_manager:**
Mount management is the plugin management for Seaflow runtime, with the main function providing dynamic plugin functionality.
+ **Node:**
Node is the basic unit of Seaflow runtime, which needs to be object-oriented encapsulated according to the objective function, supports parameter dependency, and needs to set input and output.
+ **Function:**
Function is another basic unit of Seaflow runtime that does not require object-oriented encapsulation, does not support parameter dependency, and cannot use input.


## What technologies were used

+ [Ray](https://www.ray.io)
+ [Networkx](https://networkx.org)
+ MetaProgramming(Python)


## How to get it

The master branch on GitHub is the most up to date code

<https://github.com/redblue0216/Seaflow>

Source download of release tags are available on GitHub

<https://github.com/redblue0216/Seaflow/releases/tag/seaflow_distribution_wheel>

Binaries and source distributions are available from PyPi

```bash
$ pip install shihua-seaflow
```

## License

Modified BSD (3-clause)


## Bug Reports

Bug reports can be submitted to the issue tracker at 

<https://github.com/redblue0216/Seaflow/issues>

