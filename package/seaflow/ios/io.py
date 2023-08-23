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

This is a data directory object class, with the main function of caching data and the main technology of orderdict.

- Design mode:

    nothing

- Key points:

    (1) Orderdict

- Main functions:

    (1) Data cache

Usage examples
--------------


Class description
------------------


"""



####### Load Packages ##############################################################################
####################################################################################################



from abc import ABCMeta,abstractclassmethod
import collections



####### Classes and Functions #######################################################################
###
### class:BaseDataCatalog
### ------data catalog object base class
###
### class:LocalDataCatalog
### ------local data catalog object class
###
#####################################################################################################



####### Data Catalog Object Base Class ##############################################################
#####################################################################################################



class BaseDataCatalog(metaclass=ABCMeta):
    '''Class Introduction:

        This is a data directory object base class, with the main function of caching data and the main technology of orderdict.
    '''


    def __init__(self):
        '''Attribute Function:

            Define a data directory object class attribute initialization method.

        :parameters:
            - Cache_Data_Info (OrderedDict) - Cached data information ordered dictionary. 
        '''

        self.cache_data_info = collections.OrderedDict()


    @abstractclassmethod
    def save(self,data_name,data_obj):
        '''Method Function:

            Define a method for saving data.

        :parameters: 
            - data_name (string) - dataset name.
            - data_obj (object) - dataset object.
        
        :return: 
            - result (string) - result string.      
        '''

        pass

    
    @abstractclassmethod
    def load(self,data_name):
        '''Method Function:

            Define a method for saving data.

        :parameters:
            - data_name (string) - dataset name.

        :return: 
            - data_obj (object) - dataset object.
        '''

        pass

    
    @abstractclassmethod
    def release(self,data_name):
        '''Method Function:

            Define a method for releasing data space.

        :parameters:
            - data_name (string) - dataset name.

        :return: 
            - result  (string) - result string.
        '''

        pass



####### Local Data Catalog Object Base Class ###################################################################
################################################################################################################



class LocalDataCatalog(BaseDataCatalog):
    '''Class Introduction:

        This is a data directory object class, with the main function of caching data and the main technology of orderdict.
    '''


    def __init__(self):
        '''Attribute function:

            Define a data directory object class attribute initialization method.

        :parameters:
            - cache_data_info (orderdict) - Cached data information ordered dictionary.
            - cache_data (orderdict) - Data cache ordered dictionary.
        '''

        super().__init__()
        self.cache_data = collections.OrderedDict()


    def save(self,data_name,data_obj):
        '''Method function:

            Define a method for saving data.

        :parameters:
            - data_name(str) - Dataset name.
            - data_obj (object) - Dataset object.

        :return:
            result (str): Run result information.
        '''

        self.cache_data[data_name] = data_obj
        result = '{} dataset save well done!'.format(data_name)
        print(result)

        return result


    def load(self,data_name):
        '''Method Function:

            Define a method for loading data.

        :parameters:
            - data_name (str) - Dataset name.

        :return:
            - data_obj (object) - Dataset object.
        '''

        data_obj = self.cache_data[data_name]
        result = '{} dataset load well done!'.format(data_name)
        print(result)

        return data_obj


    def release(self,data_name):
        '''Method Function:

            Define a method for releasing data space.

        :parameters:
            - data_name (str) - Dataset name.

        :return:
            - result (str) - Run result information.
        '''

        del self.cache_data[data_name]
        result = '{} dataset release well done!'.format(data_name)
        print(result)

        return result
    


#####################################################################################################################################
#####################################################################################################################################


