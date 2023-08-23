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

This is a node class that loads Function object. Its main functions are node instantiation of Function object, and the main technology is property descriptor dynamic properties

- Design mode:

    nothing

- Key points:

    (1) Property Descriptor Dynamic Properties

- Main functions:

    (1) Objectification function nodes

Usage examples
--------------


Class Description
-----------------


"""



####### Load Packages ##############################################################################
####################################################################################################






####### Classes and Functions #######################################################################
###
### class:Node
### ------node object class
###
### function:_node_error_message
### ------node error message
###
######################################################################################################



####### Node object class #####################################################################################
###############################################################################################################



from typing import Any


class Node(object):
    '''Class Introduction:

        This is a node class that loads Function object. Its main functions are node instantiation of Function object, and the main technology is property descriptor dynamic properties.
    '''


    def __init__(self,func,inputs,outputs,name):
        '''Attribute Function:

            Define a node class attribute initialization method.

        :parameters:
            - func (object) - Function object.
            - inputs (list) - Input list.
            - outputs (list) - Output list.
            - name (str) - Function name.
        '''

        ### Parametric test
        ### Verify whether func parameter is a callable Function object
        if not callable(func):
            raise ValueError(_node_error_message(msg='func argument must be a function,not {}'.format(type(func).__name__)))
        ### Verify if the inputs parameter exists and is a list, dictionary, and string
        if inputs and not isinstance(inputs,(list,dict,str)):
            raise ValueError(_node_error_message(msg='inputs argument must be one of [String,List,Dict,None],not {}'.format(type(inputs).__name__)))
        ### Verify if the outputs parameter exists and is a list, dictionary, and string
        if outputs and not isinstance(outputs,(list,dict,str)):
            raise ValueError(_node_error_message(msg='outputs argument must be one of [String,List,Dict,None],not {}'.format(type(outputs).__name__)))
        ### Verify if the name parameter exists and is a list, dictionary, and string
        if name and not isinstance(name,(list,dict,str)):
            raise ValueError(_node_error_message(msg='name argument must be one of [String,List,Dict,None],not {}'.format(type(name).__name__)))
        ### Starting to load parameters after validation
        self._func = func
        self._inputs = inputs
        self._outputs = outputs
        self._name = name


    def get_name(self):
        '''Method Function:

            Define a method for obtaining node name.

        :return:
            - name (object) - node name.
        '''

        node_name = self._name

        return node_name


    def get_func(self):
        '''Method Function:

            Define a method for obtaining node functions.

        :return:
            - node_func (object) - node function object.
        '''

        node_func = self._func

        return node_func


    def get_inputs(self):
        '''Method Function:

            Define a method for obtaining node input parameters.

        :return:
            - node_inputs (list) - node input list.
        '''

        node_inputs = self._inputs

        return node_inputs


    def get_outputs(self):
        '''Method Function:

            Define a method for obtaining a node output list.

        :return:
            - node_outputs (list) - node output list.
        '''

        node_outputs = self._outputs

        return node_outputs


    @property
    def func(self):
        '''Method Function:

            Define a descriptor protocol property dynamic property to obtain Function object method.
        '''

        return self._func


    @func.setter
    def func(self,func):
        '''Method Function:

            Define a descriptor protocol property dynamic property setting Function object method.

        :parameters:
            - func (object) - function object.
        '''

        self._func = func


    @property
    def inputs(self):
        '''Method Function:

            Define a descriptor protocol dynamic attribute acquisition input method.

        :return:
            - _inputs (list) - Parameter attribute input.
        '''

        return self._inputs


    @property
    def outputs(self):
        '''Method Function:

            Define a descriptor protocol dynamic attribute acquisition output method.

        :return:
            - _outputs (list) - Parameter attribute output.
        '''

        return self._outputs


    @property
    def name(self):
        '''Method Function:

            Define a Descriptor Protocol Dynamic Attribute Get Name Method.

        :return:
           - _name (str) - Parameter attribute name.
        '''

        return self._name


    def run(self,*args,**kwargs):
        '''Method Function:

            Define a method for running a node.

        :parameters:
            - args (tuple) - Input parameter tuples.
            - kwargs (dict) - Input parameter dictionary.

        :return:
            - run_result (object) - run the result object.
        '''

        run_result = self.func(*args,**kwargs)
        
        return run_result
    

    def __call__(self,*args,**kwargs):
        '''Method Function:

            Define a call magic method for running a node.

        :parameters:
            - args (tuple) - Input parameter tuples.
            - kwargs (dict) - Input parameter dictionary.

        :return:
            - run_result (object) - run the result object.
        '''        

        run_result = self.func(*args,**kwargs)

        return run_result
    


####### Auxiliary function set ######################################################################################################################
#####################################################################################################################################################



def _node_error_message(msg):
    '''Function Function:

        Define a function that outputs node error information.

    :parameters:
        - msg (str) - Error message.

    :return:
        - result (str) - Combined error message output.
    '''

    result_a = "Invalid Node defination:{} \n".format(msg)
    result_b = "Format should be: Node(function,inputs,outputs)"
    result = result_a + result_b

    return result



######################################################################################################################################################
######################################################################################################################################################


