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

This is a process pipeline and directed acyclic graph runtime class, with the main functions of using data caching and mounting objects to run process pipelines and directed acyclic graphs. The main technologies include networkx, metaprogramming, hook, and ray.

- Design mode:

    simple factory mode

- Key points:

    (1) network

    (2) Metaprogramming Technology

    (3) hook technology

- Main functions:

    (1) Algorithmic arrangement

    (2) Automatic optimization, including caching and heterogeneous parallel computing

Usage examples
--------------


Class Description
-----------------


"""



####### Load Packages ##############################################################################
####################################################################################################



from abc import ABCMeta,abstractclassmethod
from seaflow.nodes.node import Node
from seaflow.schedulers.dag import DAGer
from seaflow.ios.io import LocalDataCatalog
from seaflow.hooks.hook import HookManager
import ray
# import inspect



####### Classes and Functions ###################################################################################################################
###
### class:DAGBaseRunner
### ------Directed acyclic graph Process Scheduling Runner Abstract Class
###
### class:DAGRunnerFunction
### ------Specific Implementation Class of Process Directed acyclic graph Runner with function,that is a method of no parameters
###
### class:DAGRunnerNode
### ------Specific Implementation Class of Process Directed acyclic graph Runner with Node,that is a method of dependented parameters
###
################################################################################################################################################



####### Directed acyclic graph Process Scheduling Runner Abstract Class ########################################################################
################################################################################################################################################



class DAGBaseRunner(metaclass=ABCMeta):
    '''Class Introduction:

        This is an abstract class of Directed acyclic graph process scheduling runtime, which mainly uses data caching and mounting objects to run process scheduling objects. The main technical operations are instantiation.
    '''


    def __init__(self,scheduler,catalog,hook_manager):
        '''Attribute Function:

            Define an initialization method for process scheduling and running class attributes.

        :parameters:
            - scheduler (object) - Process scheduling object.
            - catalog (object) - Directory Object.
            - hook_manager (object) - Mount Object.
        '''

        self.scheduler = scheduler
        self.catalog = catalog
        self.hook_manager = hook_manager


    @abstractclassmethod
    def execute(self,is_release):
        '''Method Function:

            Define an abstract method for executing process scheduling.

        :parameters:
            - is_release (bool) - Whether to clear the cache after executing the node.
        '''

        pass



####### Directed acyclic graph Process Scheduling Runner Specific Implementation Class with Function #######################################################################################################
############################################################################################################################################################################################################



class DAGRunnerFunction(DAGBaseRunner):
    '''Class Introduction:

        This is a directed acyclic graph executor that schedules execution in a parameterless function manner. Its main functions include algorithm orchestration, automatic cache optimization, and parallel computing. Its main technologies include networkx, ray, DAG, and metaprogramming techniques.
    '''


    def __init__(self,scheduler=DAGer(),catalog=LocalDataCatalog(),hook_manager=HookManager()):
        '''Attribute Function:

            Define an initialization method for process pipeline runtime class properties, inherited from BaseRunne.

        :parameters:
            -scheduler (object) - Process scheduling object.
            -catalog (object) - Directory Object.
            -hook_manager (object) - Mount Object.
            -done_nodes (object) - Completed node collection.
            -remaining_nodes (object) - Remaining node set.
        '''

        super().__init__(scheduler=scheduler,catalog=catalog,hook_manager=hook_manager)
        self.done_nodes = set()
        self.remaining_nodes = set()


    def execute_algorithm_with_ray(self,run_list):
        '''Method Function:

            Define an execution method using the ray computing engine, including three steps: collecting parameters, executing, and pushing results.

        :parameters:
            - run_list (list) - Single Run Task List.

        :return:
            - result (str) - Execution success result information.
        '''

        ### According to run_ Get function object by executing name in list
        run_obj_list = [self.scheduler.get_function_obj(function_name=tmp_run) for tmp_run in run_list]
        ### To loop through ray calls, it is important not to enable the ray runtime context environment
        # with self.runner.runner_context:
        ### Registering functions to ray
        ray_remote_list = []
        for tmp_run_obj in run_obj_list:
            ray_remote_list.append(ray.remote(tmp_run_obj))
        ### Collect parameters based on function objects
        ### The parameter collection here needs to be placed in the actual running function and obtained from the parameter storage. (Parameter collection requires self coding and implementation in the function)
        ### Return asynchronous caller
        ray_future_list = [tmp_ray.remote() for tmp_ray in ray_remote_list]
        ### Execute Function Object
        ray_result = ray.get(ray_future_list)
        result = 'Algorithm DAG execute well done!'
        print(result)
        ### Push the result based on the function object
        ### The result push here also needs to be placed in the actual running function and pushed to the data storage end. (Data storage requires encoding and implementation in functions)
        return result


    def execute(self,init_method='kahn',dispath_method='kahn',exec_method='ray',is_release=True):
        '''Method Function:

            Define a method for executing algorithm DAG.

        :parameters:
            - cuber_controller (object) - The target controller, here is the instance itself self.
            - init_method (str) - Initialization method.
            - dispath_method (str) - Scheduling methods.
            - exec_method (str) - operating method.

        :return:
            - result (str) - Execution success result information.
        '''

        ### Step-1:Initialize Work Queue
        if init_method == 'kahn':
            init_job_queue_result = self.scheduler.init_job_queue_with_kahn()
            print('init method {} execute well done!'.format(init_method))
        else:
            print('Other init methods are developing!')
        ### Step-2:Using Kahn scheduling algorithm
        if dispath_method == 'kahn':
            dispath_iter_result = self.scheduler.dispath_algorithm_with_kahn()
            print('dispath method {} execute well done!'.format(dispath_method))
        else:
            print('Other dispath methods are developing!')
        ### Step-3:Using ray to run parallel scheduling task tables
        ### Generators generated using scheduling algorithms
        if exec_method == 'ray':
            for tmp_dispath in dispath_iter_result:
                self.execute_algorithm_with_ray(run_list=tmp_dispath)
            print('exec method {} execute well done!'.format(exec_method))
        else:
            print('Other exec methods are developing!')
        result = 'Algorithm DAG run well done!'
        print(result)

        return result



####### Directed acyclic graph Process Scheduling Runner Specific Implementation Class with Node ####################################################################################################################
#####################################################################################################################################################################################################################  



class DAGRunnerNode(DAGBaseRunner):
    '''Class Introduction:

        This is a directed acyclic graph executor that schedules execution in a node object manner that supports parameter dependency. Its main functions include algorithm orchestration, automatic optimization of caching, and parallel computing. Its main technologies include networkx, ray, DAG, and metaprogramming techniques.
    '''


    def __init__(self,scheduler=DAGer(),catalog=LocalDataCatalog(),hook_manager=HookManager()):
        '''Attribute Function:

            Define an initialization method for process pipeline runtime class properties, inherited from BaseRunne.

        :parameters:
            -scheduler (object) - Process scheduling object.
            -catalog (object) - Directory Object.
            -hook_manager (object) - Mount Object.
            -done_nodes (object) - Completed node collection.
            -remaining_nodes (object) - Remaining node set.
        '''

        super().__init__(scheduler=scheduler,catalog=catalog,hook_manager=hook_manager)
        self.done_nodes = set()
        self.remaining_nodes = set()


    def execute_algorithm_with_ray(self,run_list):
        '''Method Function:

            Define an execution method using the ray computing engine, including three steps: collecting parameters, executing, and pushing results.

        :parameters:
            - run_list (list) - Single Run Task List.

        :return:
            - result (str) - Execution success result information.
        '''

        ### According to run_ Obtain node list by executing the name in the list
        run_nodes_list = [self.scheduler.get_function_obj(function_name=tmp_run) for tmp_run in run_list]
        ### To loop through ray calls, it is important not to enable the ray runtime context environment
        # with self.runner.runner_context:
        ### Register functions to ray and collect parameters
        ray_remote_list = []
        ray_parameter_str_dict = {}
        for tmp_run_obj in run_nodes_list:
            ### Using get_ The func method obtains the function object from node and loads it into ray
            ray_remote_list.append(ray.remote(tmp_run_obj.get_func()))
            ### Collect parameters based on function objects to form a parameter string dictionary
            ray_parameter_str_dict[tmp_run_obj.get_name()] = tmp_run_obj.get_inputs()
        ### Collect parameters based on function objects to form a parameter string dictionary
        ray_parameter_obj_dict = {}
        for tmp_key,tmp_param_str_list in zip(ray_parameter_str_dict.keys(),ray_parameter_str_dict.values()):
            ### Load parameter data from catalog based on parameter name
            tmp_param_obj_list = [self.catalog.load(data_name=tmp_param) for tmp_param in tmp_param_str_list]
            ### To facilitate function calls, convert the parameter list into parameter tuples
            tmp_param_obj_tuple = tuple(tmp_param_obj_list)
            ray_parameter_obj_dict[tmp_key] = tmp_param_obj_tuple
        ### Return asynchronous caller
        ray_future_list = [tmp_ray.remote(*tmp_param_obj_tuple) for tmp_ray,tmp_param_obj_tuple in zip(ray_remote_list,ray_parameter_obj_dict.values())]
        ### Execute Function Object
        ray_result_list = ray.get(ray_future_list)
        ### Based on the function object, obtain the output name and push the result to the catalog accordingly
        for tmp_run_obj,tmp_ray_result in zip(run_nodes_list,ray_result_list):
            tmp_output_name = tmp_run_obj.get_outputs() ### Only one output is temporarily guaranteed here
            self.catalog.save(data_name=tmp_output_name,data_obj=tmp_ray_result)
        result = 'Algorithm DAG execute well done!'
        print(result)

        return ray_result_list


    def execute(self,init_method='kahn',dispath_method='kahn',exec_method='ray',is_release=True):
        '''Method Function:

            Define a method for executing algorithm DAG.

        :parameters:
            - cuber_controller (object) - The target controller, here is the instance itself self.
            - init_method (str) - Initialization method.
            - dispath_method (str) - Scheduling methods.
            - exec_method (str) - operating method.

        :return:
            - result (str) - Execution success result information.
        '''

        ### Step-1:Initialize Work Queue
        if init_method == 'kahn':
            init_job_queue_result = self.scheduler.init_job_queue_with_kahn()
            print('init method {} execute well done!'.format(init_method))
        else:
            print('Other init methods are developing!')
        ### Step-2:Using Kahn scheduling algorithm
        if dispath_method == 'kahn':
            dispath_iter_result = self.scheduler.dispath_algorithm_with_kahn()
            print('dispath method {} execute well done!'.format(dispath_method))
        else:
            print('Other dispath methods are developing!')
        ### Step-3:Using ray to run parallel scheduling task tables
        ### Generators generated using scheduling algorithms
        if exec_method == 'ray':
            for tmp_dispath in dispath_iter_result:
                self.execute_algorithm_with_ray(run_list=tmp_dispath)
            print('exec method {} execute well done!'.format(exec_method))
        else:
            print('Other exec methods are developing!')
        result = 'Algorithm DAG run well done!'
        print(result)

        return result
    


#############################################################################################################################################################
#############################################################################################################################################################


