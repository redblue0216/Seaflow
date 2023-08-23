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

This is a hook mount management class, with the main function of managing the mount hook functions and the main technical static methods.

- Design mode:

    nothing

- Key points:

    (1) Hook technology

- Main functions:

    (1) Plug-in Manager

Usage examples
--------------


Class description
------------------


"""



####### Load Packages ##############################################################################
####################################################################################################



import pluggy
import collections



####### Classes and Functions #######################################################################
###
### class:LocalDatacatalogSpecs
### ------local data catalog interface class
###
### class:LocalDataCatalogPlugin
### ------local data catalog plugin class
###
### class:HookManager
### ------hook mount management class
###
#####################################################################################################



###### hook-markers tool class ###########################################################################
##########################################################################################################



HOOK_NAMESPACE = 'seaflow'
hook_spec = pluggy.HookspecMarker(HOOK_NAMESPACE)
hook_impl = pluggy.HookimplMarker(HOOK_NAMESPACE)



####### hook-specs interface class ####################################################################################
#######################################################################################################################



class LocalDatacatalogSpecs(object):
    '''Class Introduction:

        This is a local data directory interface class.
    '''

    @hook_spec
    def collect_parameter_data_before_node_run_locally(self,catalog,inputs):
        ''' Method Function:

            Define a method for collecting parameters before running.

        :parameters:
            - catalog (object) - Data directory object.
            - inputs (list) - List of input parameters.

        :return:
            - data_dict (dict) - Data dictionary.
        '''

        pass


    @hook_spec
    def store_cache_data_after_node_run_locally(self,catalog,outputs,data_objects):
        '''Method function:

            Define a method for saving cached data after running.

        :parameters:
            - catalog (object) - Data directory object.
            - outputs (list) - Output parameter list.
            - data_objects (list) - List of data objects.

        :return:
            - result (str) - Run result information.
        '''

        pass



###### hook-impl realization class #############################################################################
################################################################################################################



class LocalDataCatalogPlugin(object):
    '''Class Introduction:

        This is a local data directory plugin class, mainly used for data caching, saving, and loading operations.
    '''

    @hook_impl
    def collect_parameter_data_before_node_run_locally(self,catalog,inputs):
        ''' Method Function:

            Define a method for collecting parameters before running.

        :parameters:
            - catalog (object) - Data directory object.
            - inputs (list) - List of input parameters.
        
        :return:
            - data_dict (dict) - Data dictionary.
        '''

        data_dict = collections.OrderedDict()
        for tmp_input in inputs:
            tmp_data = catalog.cache_data[tmp_input]
            data_dict[tmp_input] = tmp_data

        return data_dict


    @hook_impl
    def store_cache_data_after_node_run_locally(self,catalog,outputs,data_objects):
        '''Method function:

            Define a method for saving cached data after running.

        :parameters:
            - catalog (object) - Data directory object.
            - outputs (list) - Output parameter list.
            - data_objects (list) - List of data objects.

        :return:
            - result (str) - Run result information.
        '''

        for tmp_output in zip(outputs,data_objects):
            tmp_name = tmp_output[0]
            tmp_data = tmp_output[1]
            catalog.cache_data[tmp_name] = tmp_data
        result = 'store cache data well done!'
        print(result)

        return result  
    


####### hook mount management class ##############################################################################
##################################################################################################################



class HookManager(object):
    '''Class Introduction:

        This is a hook mount management class, with the main function of managing the mount hook functions and the main technical static methods.
    '''


    def __init__(self):
        '''Attribute Function:

            Define an attribute function initialization method.

        :parameters:
            - hook_manager_instance (object) - Hook Mount Management Object Instance.
        '''

        self.hook_manager_instance = self.get_hook_manager_instance()
        self.hook = self.hook_manager_instance.hook


    def create_hook_manager(self):
        '''Method Function:

            Define a method for creating a mount manager.

        :return:
            - hook_manager (object) - hook manager object.
        '''

        hook_manager = pluggy.PluginManager(HOOK_NAMESPACE)

        return hook_manager


    def register_plugins(self,hook_manager):
        '''Method Function:

            Define a method for registering plugin objects.

        :parameters:
            - hook_manager (object) - hook manager object.

        :return:
            - hook_manager (object) - The hook manager object for which the plugin object has been registered.
        '''

        ### The plugin here temporarily only supports the function of local data directory, and other practical functions will be added in the future.
        ### Add Mount Interface
        hook_manager.add_hookspecs(LocalDatacatalogSpecs)
        ### Registration Mount Implementation
        hook_manager.register(LocalDataCatalogPlugin()) ### Must add () to open the object's callable

        return hook_manager


    def get_hook_manager_instance(self):
        '''Method Function:

            Define a method for obtaining mount manager instances

        :return:
            - hook_manager (object) - The hook manager object that has been matched with the relevant plugin object.
        '''

        void_hook_manager = self.create_hook_manager()
        registed_hook_manager = self.register_plugins(hook_manager = void_hook_manager)

        return registed_hook_manager
    


##################################################################################################################################################
##################################################################################################################################################


