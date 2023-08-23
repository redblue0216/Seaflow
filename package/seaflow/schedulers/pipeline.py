# -*- coding: utf-8 -*-
# Author:shihua
# Designer:shihua
# Coder:shihua
# Email:15021408795@163.com
# License: BSD 3 clause
# Copyright (c) 2023 The SeaFlow Authors. All rights reserved.



"""
Module Introduction
-------------------

This is a scheduling management class that mainly combines nodes to form execution processes; Main technical internal methods and magic methods.

- Design mode:

    nothing

- Key points:

    (1) Attribute function

- Main functions:

    (1) Function node management

Usage examples
--------------


Class Description
-----------------


"""



####### Load Packages ##############################################################################
####################################################################################################



import collections



####### Classes and Functions #######################################################################
###
### class:Pipelene
### ------process pipeline class
###
######################################################################################################



###### Process Pipeline Class ######################################################################
####################################################################################################



class Pipeline(object):
    '''Class Introduction:

        This is a process management class that mainly combines nodes to form execution processes; Main technical internal methods and magic methods.
    '''


    def __init__(self,nodes):
        '''Attribute Function:

            Define an initialization method for process pipeline class attributes.

        :parameters:
            - nodes (list) - Node Object List (Ordered).
            - _nodes_by_name (list) - Node Name List (Ordered).
            - _nodes_by_input (orderdict) - Node Input List (Ordered).
            - _nodes_by_output (orderdict) - Node output list (ordered).
        '''

        ### Verify input parameters
        if nodes is None:
            raise ValueError("nodes argument of pipeline is None.It must be an iterable of nodes and/or pipeline instead.")
        if nodes and not isinstance(nodes,(list)):
            raise ValueError("nodes argument of pipeline is not list.It must be an list of nodes and/or pipeline instead.")
        ### Mount Node List
        nodes = list(nodes)
        self.nodes = nodes
        ### Parsing loaded nodes to generate corresponding information data lists
        ### _nodes_by_name
        _nodes_by_name_list = [tmp_node.name for tmp_node in nodes]
        ### _nodes_by_input,_nodes_by_output
        _nodes_by_inputs_orderdict = collections.OrderedDict()
        _nodes_by_outputs_orderdict = collections.OrderedDict()
        for tmp_node in nodes:
            _nodes_by_inputs_orderdict[tmp_node.name] = tmp_node.inputs
            _nodes_by_outputs_orderdict[tmp_node.name] = tmp_node.outputs
        ### Load Name, Input, and Output List
        self._nodes_by_name = _nodes_by_name_list
        self._nodes_by_inputs = _nodes_by_inputs_orderdict
        self._nodes_by_outputs = _nodes_by_outputs_orderdict


    def __add__(self,other):
        '''Method Function:

            The Magic Method of Defining a Plus Operator.

        :parameters:
            - other (object) - Another process pipeline object.

        :return:
            - new_pipeline (object) - Newly combined process pipeline objects.
        '''

        ### Verify Parameter Object
        if not isinstance(other,Pipeline):
            return NotImplemented
        ### Splicing Two Process Pipeline Objects
        new_nodes = list(self.nodes + other.nodes)
        new_pipeline = Pipeline(nodes = new_nodes)

        return new_pipeline


    def names(self):
        '''Method Function:

            Define a method for enumerating node names in a process pipeline.

        :return:
            - names (list) - Node Name List.
        '''

        return self._nodes_by_name


    def inputs(self):
        '''Method Function:

            Define a method for enumerating input parameters in a process pipeline.

        :return:
            - inputs (orderdict) - Ordered Dictionary of Input Parameters.
        '''

        return self._nodes_by_inputs


    def outputs(self):
        '''Method Function:

            Method of defining a grid to list output parameters in a process pipeline.

        :return:
            - outputs (orderdict) - Output Parameter Ordered Dictionary
        '''

        return self._nodes_by_outputs
    


#####################################################################################################################################
#####################################################################################################################################


