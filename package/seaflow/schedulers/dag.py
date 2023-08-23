# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: BSD 3 clause
# Copyright (c) 2023 The SeaFlow Authors. All rights reserved.



'''
Module Introduction
-------------------

This is a directed acyclic graph management class, which mainly manages directed acyclic graphs, including CRUD operations related to directed acyclic graphs and scheduling operations based on directed acyclic graphs; Main technologies: Networkx, descriptor technology, Kahn hierarchical algorithm, decorator technology and magic method of Operator overloading, prepare and new construction process.

- Design mode:

    nothing

- Key points:

    (1) Networkx

    (2) Descriptor technology

    (3) Kahn algorithm

    (4) Prepare and New Construction Process Magic Method

    (5) Decorator Technology

    (6) Operator overloading

    (7) Type meta

    (8) Call magic method

    (9) Get magic method

    (10) Deque technology

- Main functions:

    (1) CRUD Operation of Directed acyclic graph

    (2) Scheduling operations of Directed acyclic graph

Usage examples
--------------


Class Description
-----------------

'''



####### Load Packages ##############################################################################
####################################################################################################


### graph
from collections import OrderedDict
import networkx as nx
### dispath
from collections import deque
### DAG
import functools
from types import MethodType
import matplotlib.pyplot as plt
### other
from seaflow.nodes.node import Node



####### Classes and Functions #######################################################################
###
### class:DAGManagerDict
### ------DAG diagram dedicated management Data dictionary
###
### class:DirectedAcyclicGraph
### ------DAG Management Class
###
### class:KahnScheduler
### ------KahnScheduler scheduling management class
###
### class:DAGerMeta
### ------Metaclass of Directed acyclic graph controllers
###
### class:DAGer
### ------Concrete implementation class of Directed acyclic graph controller
###
######################################################################################################



####### DAG diagram dedicated management Data dictionary ###########################################################################
####################################################################################################################################



class DAGManagerDict(OrderedDict):
    '''Class Introduction:

        Define a DAG diagram dedicated management Data dictionary and inherit the ordered dictionary.
    '''

    def __init__(self,*args,**kwargs):
        '''Attribute Function:

            Define a method for initializing DAG management dictionaries.
        '''

        super(DAGManagerDict,self).__init__(*args,**kwargs)


    def __getattr__(self,key):
        '''Method Function:

            Define a magic method for obtaining attributes.

        :parameters:
            - key (str) - The name of the dictionary key.

        :return:
            - value (object) - The value object corresponding to the dictionary key.
        '''

        value = self[key]
        if isinstance(value,OrderedDict):
            value = DAGManagerDict(value)
        elif isinstance(value,dict):
            value = DAGManagerDict(value)
        return value



####### DAG Management Class ##################################################################################
###############################################################################################################



class DirectedAcyclicGraph(object):
    '''Class Introduction:

        This is a DAG management class, mainly used to manage DAG, mainly using Networkx technology and descriptor protocols.
    '''


    def __init__(self,name):
        '''Attribute Function:

            Define an initialization method, mainly used to define the DAG manager name.

        :parameters:
            - name (str) - name.
            - graph (DAGManagerDict) - DAG Manager Dictionary Object.
        '''

        self.name = name
        graph = DAGManagerDict()
        graph['name'] = 'DAG'
        graph['remarks'] = 'no params'
        graph['obj'] = nx.DiGraph()
        self.graph = graph


    def __get__(self,instance,owner):
        '''Method Function:

            Overloading a magic method for obtaining class objects.

        :parameters:
            - instance (object) - Managed class, which declares a descriptor class as a class attribute.
            - owner (object) - Managed instances, i.e. instance objects of managed classes.
        '''

        return self.graph
    


####### KahnScheduler scheduling management class ##################################################################################
####################################################################################################################################



class KahnScheduler(object):
    '''Class Introduction:

        This is a task scheduler class, with main functions based on DAG calculation graphs and scheduling calculations using a computing engine. The main techniques include hierarchical queues and scheduling algorithms.
    '''


    def __init__(self):
        '''Attribute Function:

            Define an initialization method for generating work queues.
        '''    

        self.job_queue = deque()
        print('KahnScheduler create success!')    



####### Metaclass of Directed acyclic graph controllers #####################################################################
#############################################################################################################################



class DAGerMeta(type):
    '''Class Introduction:

        This is a control metaclass of Ensemble learning calculation chart. Its main function is to manage the generation of Ensemble learning calculation chart control class. Its main technology__Prepare__,__New__.
    '''


    @classmethod
    def __prepare__(cls,name,bases):
        '''Method Function:

            Overload a class creation preparation method to implement ordered attribute methods.

        :parameters:
            - name (str) - class name.
            - bases (tuple) - Base class tuple.

        :return:
            - OrderedDict (dict) - A namespace variable defined within a class, using an ordered dictionary to standardize the order of class attributes.
        '''

        return OrderedDict()


    def __new__(cls,name,bases,attrs_dict,*args,**kwargs):
        '''Method Function:

            Overload a class creation method to implement DAG management objects based on Networkx.

        :parameters:
            - name (str) - class name.
            - bases (tuple) - Base calss tuple.
            - attrs_dict (dict) - A namespace variable defined within a class, starting from__Prepare__ Method return access.

        :return:
            - instance (object) - Create an instance of the class (actually an object).
        '''

        attrs_dict['manager'] = DirectedAcyclicGraph('DirectedAcyclicGraph')
        cls.instance = type.__new__(cls,name,bases,attrs_dict,*args,**kwargs)

        return cls.instance   



####### Concrete implementation class of Directed acyclic graph controller #####################################################################
################################################################################################################################################



class DAGer(metaclass = DAGerMeta):
    '''Class Introduction:

        This is a Directed acyclic graph control class. Its main function is to provide the management of DAG, computing engine management and scheduling management. The main technologies are based on DAG technology of Networkx, Ray based computing environment, decorator technology and Operator overloading.
    '''


    def __init__(self,scheduler=KahnScheduler()): ### Use KahnScheduler as the default scheduler.
        '''Attribute Function:

            Define an initial attribute method, mainly used to load the calculation engine and scheduling engine when creating control objects.

        :parameters:
            - dag (object) - DAG Graph Management Objects.
            - scheduler (object) - Scheduling Engine Object.
        '''

        self.scheduler = scheduler


    def add_node(self,method='single',node_name=None,data_obj=None,data_dict_list=None,*args,**kwargs):
        '''Method Function:

            Define a method for adding nodes.

        :parameters:
            - node_name (str) - node name.
            - data_obj (obj) - node data object.
            - method (str) - Add Method.
             - data_dict_list (list) - List of Data dictionary.

        :return:
            - result (str): Return result text.
        '''

        if method == 'single':
            self.manager.obj.add_node(node_name,data=data_obj)
        elif method == 'batch':
            self.manager.obj.add_nodes_from(data_dict_list)
        result = 'Algorithm node add well done!'
        print(result)
        return result


    def add_edge(self,method='single',start_node_name=None,end_node_name=None,data_obj=None,data_dict_list=None,*args,**kwargs):
        '''Method Function:

            Define a method for adding edges.

        :parameters:
            - start_node_name (str) - Start Node Name.
            - end_node_name (str) - End Node Name.
            - data_obj (object) - Node Data Object.
            - method (str) - Add Method.
            - data_dict_list (list) - List of Data dictionary.
        :return:
            - result (str) - Return result text.
        '''

        if method == 'single':
            self.manager.obj.add_edge(start_node_name,end_node_name,data=data_obj,*args,**kwargs)
        elif method == 'batch':
            self.manager.obj.add_edges_from(data_dict_list)
        result = 'Algorithm edge add well done!'
        print(result)
        return result


    def modify_node(self,node_name=None,data_obj=None,*args,**kwargs):
        '''Method Function:

            Define a method for modifying nodes.

        :parameters:
            - node_name (str) - Node name.
            - data_obj (object) - Node data object.

        :return:
            - result (str) - Return result text.
        '''

        self.manager.obj.nodes[node_name]['data'] = data_obj
        result = 'Algorithm node modify well done!'
        print(result)


    def modify_edge(self,start_node_name=None,end_node_name=None,data_obj=None,*args,**kwargs):
        '''Method Function:

            Define a method for modifying edges.

        :parameters:
            - start_node_name (str) - Start Node Name.
            - end_node_name (str) - End Node Name.
            - data_obj (object) - Node Data Object.

        :return:
            - result (str) - Return result text.
        '''

        self.manager.obj.edges[start_node_name,end_node_name]['data'] = data_obj
        result = 'Algorothm edge modify well done!'
        print(result)


    def remove_node(self,method='single',node_name=None,nodes_name_list=None,*args,**kwargs):
        '''Method Function:

            Define a method for deleting nodes.

        :parameters:
            - method (str) - Delete Method.
            - node_name (str) - node name.
            - nodes_name_list (list) - Node Name List.

        :return:
            - result (str) - Return result text.
        '''

        if method == 'single':
            self.manager.obj.remove_node(node_name)
        elif method == 'batch':
            self.manager.obj.remove_nodes_from(nodes_name_list)
        result = 'Algorithm node remove well done!'
        print(result)
        

    def remove_edge(self,method='single',start_node_name=None,end_node_name=None,edges_name_list=None,*args,**kwargs):
        '''Method Function:

            Define a method for deleting edges.

        :parameters:
            - method (str) - Delete Method.
            - edge_name (str) - Edge Name.
            - edges_name_list (list) - Edge Name List.

        :return:
            - result (str) - Return result text.
        '''

        if method == 'single':
            self.manager.obj.remove_edge(start_node_name,end_node_name)
        elif method == 'batch':
            self.manager.obj.remove_edges_from(edges_name_list)
        result = 'Algorithm edge remove well done!'
        print(result)


    def query_node(self,node_name=None,*args,**kwargs):
        '''Method Function:

            Method for defining a query graph node.

        :parameters:
            - node_name (str) - Node name.

        :return:
            - data_dict (dict) - Data Dict.
        '''

        data_dict = self.manager.obj.nodes[node_name]
        print('Algorithm node query well done!')
        return data_dict


    def query_edge(self,start_node_name=None,end_node_name=None,*args,**kwargs):
        '''Method Function:

            Method of Defining a Query Graph Edge.

        :parameters:
            - start_node_name (str) - Start Node Name.
            - end_node_name (str) - End Node Name.

        :return:
            - data_dict (str) - Data Dict.
        '''

        data_dict = self.manager.obj.edges[start_node_name,end_node_name]
        print('Algorithm edge query well done!')
        return data_dict


    def get_node_neighbor(self,node_name=None,direction=None,*args,**kwargs):
        '''Method Function:

            Define a method for accessing neighboring nodes of the target node.

        :parameters:
            - node_name (str) - Node name.
            - direction (str) - Access direction.

        :return:
            - nodes_list (list) - Nodes list.
        '''

        if direction == 'backward':
            nodes_list = list(self.manager.obj.successors(n=node_name))
        elif direction == 'forward':
            nodes_list = list(self.manager.obj.predecessors(n=node_name))
        print('Algorithm get node neighbor well done!')
        return nodes_list 


    def get_edge_neighbor(self,node_name=None,direction=None,*args,**kwargs):
        '''Method Function:

            Define a method for accessing neighboring edges of target nodes.

        :parameters:
            - node_name (str) - Node name.
            - direction (str) - Access direction.

        :return:
            - edges_list (list) - Edges list.
        '''

        if direction == 'backward':
            edges_list = list(self.manager.obj.out_edges(nbunch=node_name))
        elif direction == 'forward':
            edges_list = list (self.manager.obj.in_edges(nbunch=node_name))
        print('Algorithm get edge neighbor well done!')
        return edges_list


    def get_node_attributes(self,node_name=None,attr_name=None,*args,**kwargs):
        '''Method Function:

            Define a method for obtaining node attributes.
        
        :parameters:
            - node_name (str) - Node name.
            - attr_name (str) - Attribute Name.

        :return:
            - atrr_data (object) - Attribute data.
        '''

        attr_data= self.manager.obj.nodes[node_name][attr_name]
        print('Algorithm node get attributes well done!')
        return attr_data


    def get_edge_attributes(self,start_node_name=None,end_node_name=None,attr_name=None,*args,**kwargs):
        '''Method Function:

            Define a method for obtaining edge attributes.

        :parameters:
            - start_node_name (str) - Start Node Name.
            - end_node_name (str) - End Node Name.
            - attr_name (str) - Attribute Name.

        :return:
            attr_data (object) - Attribute data.
        '''

        attr_data = self.manager.obj.edges[start_node_name,end_node_name][attr_name]
        print('Algorithm edge get attributes well done!')
        return attr_data


    def get_function_obj(self,function_name):
        '''Method Function:

            Define a method to obtain the Function object.

        :parameters:
            - function_name (str) - Function name.

        :return:
            - function_obj (object) - Function object.
        '''

        function_obj = self.manager.obj.nodes[function_name]['data']
        print('Algorithm function object get well done!')
        return function_obj


    def show_nodes(self):
        '''Method Function:

            Define a method to display the node situation of the graph manager.

        :return:
            - show_nodes_list (List) - Display node list.
        '''

        show_nodes_list = self.manager.obj.nodes
        print('Algorithm show nodes well done!')
        return show_nodes_list


    def show_edges(self):
        '''Method Function:

            A method for defining the edge situation of a presentation graph manager.

        :return:
            - show_edges_list (List) - Show Edge List.
        '''

        show_edges_list = self.manager.obj.edges
        print('Algorithm show edges well done!')
        return show_edges_list


    def get_graph_obj(self):
        '''Method Function:

            Define a method for obtaining graph objects.

        :return:
            - graph_obj (object) - Graph object, based on networkx technology.
        '''

        graph_obj = self.manager.obj
        print('Algorithm get graph object well done!')
        return graph_obj


    def toposort_algorithm(self):
        '''Method Function:

            Define a method for Topological sorting.

        :return:
            - toposort_list (list) - Topological sorting List.
        '''

        toposort_list = list(nx.topological_sort(self.manager.obj))        
        print('Algorithm toposort well done!')
        return toposort_list


    def kahnclassification_algorithm(self):
        '''Method Function:

            Define a DAG graph layering algorithm.

        :return:
            - kahnclassification_list (list) - Kahn Rating List.
        '''
        
        ### 运行kahn分级算法得到运行节点优先级分层
        kahnclassification_list = list(nx.topological_generations(self.manager.obj))
        print('Algorithm kahn run well done!')
        return kahnclassification_list


    def draw(self):
        '''Method Function:

            Define a method for displaying legends.
        '''

        nx.draw(self.manager.obj,with_labels=True)
        plt.draw()
        plt.show()


    def register(self,controller_obj):
        '''Method Function:

            Define a decorator method for registering functions, using the decorator function technique with parameters.

        :parameters:
            - controller_obj (object) - controller object.

        :return:
            - decorate (function) - Decorator Function object.
        '''


        def decorate(func):
            '''Method Function:

                Define a decorator function.

            :parameters:
                - func (function) - Objective decoration function.

            :return:
                - extand_func (object) - Expanded Function object.
            '''



            class Extand(object):
                '''Class Introduction:

                    This is a registration function class mainly used to register the target function into the graph manager.
                '''

                
                def __init__(self,func):
                    '''Attribute Function:

                        Define a method for initializing and registering function classes, mainly used to register functions into the graph manager.

                    :parameters:
                        - func (object) - Function object.
                    '''

                    ### Place the objective function in the extend attribute.
                    self.func = func
                    ### Obtain the target function name and object.(function and node object)
                    ### distinguish function and node object
                    if type(func) == Node:
                        tmp_node_name = func.name
                    else:
                        tmp_node_name = func.__name__
                    tmp_data_obj = func
                    ### Register the objective function on the controller.
                    controller_obj.add_node(method='single',
                                            node_name=tmp_node_name,
                                            data_obj=tmp_data_obj)
                    # print(controller_obj.manager.obj)
                    # print(controller_obj.manager.obj.nodes)
                    ### Referencing Target Function Properties.
                    functools.wraps(func)(self)
                    ### distinguish function and node object
                    if type(func) == Node:
                        print('Algorithm {} node added to the graph manager well done!'.format(self.func.name))
                    else:
                        print('Algorithm {} node added to the graph manager well done!'.format(self.func.__name__))


                def __call__(self,*args,**kwargs):
                    '''Method Function:

                        Overloading a magic method called by an instance.

                    :return:
                        - Function Run Result.
                    '''

                    return self.func(*args,**kwargs)


                def __get__(self,instance,cls):
                    '''Method Function:

                        Overloading a function instance to create and obtain methods.

                    :parameters:
                        - instance (object) - Affiliated instance.
                        - cls (object) - Category.

                    :return:
                        - self (object) - Function object instance itself.
                    '''

                    if instance is None:
                        return self
                    else:
                        return MethodType(self,instance)


                def __rshift__(self,other):
                    '''Method Function:

                        Overloading a right shift operator is mainly used to add edges to algorithm nodes and construct algorithm dependencies.

                    :parameters:
                        - self (object) - Self Function object instance.
                        - other (object) - Another instance of a Function object.

                    :return:
                        - other (object) - For another function object instance, in order to use the shift operator continuously, only the end nodes can be passed in turn.
                    '''
                    
                    ### distinguish function and node object
                    if type(self.func) == Node:
                        tmp_start_node_name = self.func.name
                    else:
                        tmp_start_node_name = self.func.__name__
                    if type(other.func) == Node:
                        tmp_end_node_name = other.func.name
                    else:
                        tmp_end_node_name = other.func.__name__
                    controller_obj.add_edge(method='single',
                                           start_node_name=tmp_start_node_name,
                                           end_node_name=tmp_end_node_name,
                                           data_obj='{} to {}'.format(tmp_start_node_name,tmp_end_node_name))
                    print('Algorithm edge {} to {} added to the graph well done!'.format(tmp_start_node_name,tmp_end_node_name))
                    return other



            ### Use Extend to expand the target Function object and return the expanded target Function object.
            extand_func = Extand(func=func)

            return extand_func

        return decorate


    ### The following methods are divided into three categories: initialization task queue, scheduling, and execution. The subsequent total execution methods are specifically composed of the corresponding versions of initialization task queue, scheduling, and execution of various small functions.
    def init_job_queue_with_kahn(self):
        '''Method Function:

            Define a method for initializing priority scheduling queues using the Kahn algorithm.

        :return:
            - result (str) - Return result text.
        '''

        ### Run the Kahn classification algorithm to obtain the priority hierarchy of the running nodes.
        kahnclassification_list = self.kahnclassification_algorithm()
        ### Add the secondary elements in the nested list to the shscheduler according to the priority, and use the list Iterator
        for tmp_kahn_item in kahnclassification_list:
            self.scheduler.job_queue.append(tmp_kahn_item)
        result = 'Init job queue well done!'
        print(result)
        return result


    def dispath_algorithm_with_kahn(self):
        '''Method Function:

            Define a scheduling method using Kahn algorithm, where yield technique is used to return a generator.

        :return:
            - run_list_iter (iter) - kahn run list iter.
        '''

        iter_num = len(self.scheduler.job_queue)
        for i in range(0,iter_num,1):
            run_list = self.scheduler.job_queue.popleft()
            print('Algorithm run task {} generate well done!'.format(run_list))
            yield run_list



########################################################################################################################################
########################################################################################################################################            


