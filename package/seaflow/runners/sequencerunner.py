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

This is a process scheduling and running class, with the main functions of using data caching and mounting objects to run process pipeline objects. The main technical operations are instantiation.

- Design mode:

    nothing

- Key points:

    (1) Data caching and hook technology

- Main functions:

    (1) Process Scheduling Runner

Usage examples
--------------


Class Description
-----------------


"""



####### Load Packages ##############################################################################
####################################################################################################



from abc import ABCMeta,abstractclassmethod
from seaflow.nodes.node import Node
from seaflow.ios.io import LocalDataCatalog
from seaflow.hooks.hook import HookManager
from seaflow.schedulers.pipeline import Pipeline



####### Classes and Functions #######################################################################
###
### class:BaseRunner
### ------Process Scheduling Runner Abstract Class
###
### class:SequentialRunner
### ------Specific Implementation Class of Process Pipeline Runner
###
######################################################################################################



####### Process Scheduling Runner Abstract Class ########################################################################
#########################################################################################################################



class BaseRunner(metaclass=ABCMeta):
    '''Class Introduction:

        This is an abstract class of process scheduling runtime, which mainly uses data caching and mounting objects to run process scheduling objects. The main technical operations are instantiation.
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



####### Specific Implementation Class of Process Pipeline Runner ##################################################################
###################################################################################################################################



class SequentialRunner(BaseRunner):
    '''Class Introduction:

        This is a specific implementation class for a process pipeline runner, which mainly uses data caching and mounting objects to run process pipeline objects. The main technical operations are instantiation.
    '''


    def __init__(self,scheduler,catalog,hook_manager):
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


    def execute(self,is_release=True):
        '''Method Function:

            Defining a specific implementation method for executing a process pipeline requires the use of scheduler objects, catalog objects, and hooks_ Manager object.

        :parameters:
            - is_release (bool) - Whether to clear the cache after executing the node.

        :return:
            - result (str) - Running result information.
        '''

        ### Obtain the list of nodes loaded by the scheduler
        nodes = self.scheduler.nodes
        ### In the__ Init__ Done pre created in_ Nodes and retaining_ Dones
        ### Start loop execution of scheduler node
        for exec_index,node in enumerate(nodes):
            print('==========>>>>>> starting execute {} node'.format(exec_index))
            try:
                ### step-1-collecting parameter
                ### Get input list
                required_inputs_list = node.get_inputs()
                ### Get the corresponding input parameter data from the cache according to the input list and integrate it into an input Data dictionary
                # required_input_data_dict = {}
                # for required_input in required_inputs_list:
                #     required_data = self.catalog.load(data_name=required_input)
                #     required_input_data_dict[required_input] = required_data
                hook_manager = HookManager()
                ### The following involves the hook function operations related to the data directory datacatalog, and currently only local operations are supported
                ### Obtain data directory type
                data_catalog_type = type(self.catalog).__name__
                if data_catalog_type == 'LocalDataCatalog':
                    collected_data_dict = hook_manager.hook.collect_parameter_data_before_node_run_locally(catalog=self.catalog,inputs=required_inputs_list)
                ### step-2-run function
                required_input_data_dict = collected_data_dict[0]
                node_output_data = node.run(**required_input_data_dict)
                ### step-3-cache result
                ### Obtain a list of output names
                # node_output_list = node.get_outputs()
                # self.catalog.save(data_name=node_output_list[0],data_obj=node_output_data) ### The default output here is an object, so only the 0 index of the output list is used
                node_output_name = node.get_outputs()
                ### The following involves the hook function operations related to the data directory datacatalog, and currently only local operations are supported
                ### Obtain data directory type
                data_catalog_type = type(self.catalog).__name__
                if data_catalog_type == 'LocalDataCatalog':
                    hook_manager.hook.store_cache_data_after_node_run_locally(catalog=self.catalog,outputs=node_output_name,data_objects=[node_output_data])### The default output here is an object, so only the 0 index of the output list is used; And the data object is loaded using a list.
                ### Adjust the node running state set done_ Nodes and retaining_ Done
                self.done_nodes.add(node)
                print('==========>>>>>> execute {} node well done!'.format(exec_index))
            except Exception:
                self.remaining_nodes = set(nodes) - set(self.done_nodes)
                raise
            ### Clear used input parameter data cache
            if is_release == True:
                for release_input in required_inputs_list:
                    self.catalog.release(data_name=release_input)



####################################################################################################################################################################################
####################################################################################################################################################################################


