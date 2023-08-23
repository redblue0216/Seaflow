==========
QuickStart
==========
Seaflow usage can be divided into three major steps. The first step is to configure the executor and generate instances, including scheduler configuration, data directory configuration, and plugin mounting; The second step is to register the object directly or register the node after objectification; Step 3: Process the registered callable objects; Step 4: Start the actuator directly.

The following is an example of Seaflow usage,

.. code-block:: python

    from seaflow.nodes.node import Node
    from seaflow.runners.dagrunner import DAGRunnerNode
    import time

    ### Declare Node Executor Instance
    dagrunner = DAGRunnerNode()
    ### function
    def test_a(a):
        time.sleep(3)
        return a + 2

    def test_aa(aa):
        time.sleep(3)
        return aa + 3

    def test_b(a1,aa1):
        time.sleep(2)
        return a1 + aa1 + 2

    def test_bb(bb):
        time.sleep(3)
        return bb + 4

    def test_c(b1,bb1):
        time.sleep(3)
        return b1 + bb1 + 2

    def test_cc(cc):
        time.sleep(3)
        return cc + 5

    def test_d(b1,c1,cc1,f1):
        time.sleep(3)
        return b1 + c1 + cc1 + f1

    def first_func(a,b):
        time.sleep(3)
        c = a + b
        return c


    ### Node objectification 
    test_a_node = Node(func=test_a,inputs=['a'],outputs='a1',name='test_a') #
    test_aa_node = Node(func=test_aa,inputs=['aa'],outputs='aa1',name='test_aa') # 
    test_b_node = Node(func=test_b,inputs=['a1','aa1'],outputs='b1',name='test_b') ##
    test_bb_node = Node(func=test_bb,inputs=['bb'],outputs='bb1',name='test_bb') #
    test_c_node = Node(func=test_c,inputs=['b1','bb1'],outputs='c1',name='test_c') ## 
    test_cc_node = Node(func=test_cc,inputs=['cc'],outputs='cc1',name='test_cc') # 
    test_d_node = Node(func=test_d,inputs=['b1','c1','cc1','f1'],outputs='d1',name='test_d') ####
    first_node = Node(func=first_func,inputs=['f','cc1'],outputs='f1',name='first_func') ##


    ### Register on scheduler
    test_a = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_a_node)
    test_aa = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_aa_node)
    test_b = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_b_node)
    test_bb = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_bb_node)
    test_c = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_c_node)
    test_cc = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_cc_node)
    test_d = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_d_node)
    first_func= dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(first_node)


    ### Registering target function dependencies to scheduler
    test_a >> test_b >> test_c >> test_d
    test_aa >> test_b >> test_d
    test_bb >> test_c >> test_d
    test_cc >> test_d
    test_cc >> first_func >> test_d


    ### Test the call of the node
    print('------>>>',first_node(100,10),first_node.run(100,10))
    print(type(first_node)==Node)


    ### Show nodes and edges
    print('~~~~~~',dagrunner.scheduler.get_graph_obj())
    print('------',dagrunner.scheduler.show_nodes())
    print('======',dagrunner.scheduler.show_edges())


    ### Show DAG diagram situation
    # dagrunner.scheduler.draw()


    ### Calling the target function
    tmp_func_a = dagrunner.scheduler.get_function_obj(function_name='test_aa')
    print(type(tmp_func_a))
    tmp_func_b = dagrunner.scheduler.get_function_obj(function_name='first_func')
    print(tmp_func_a(10))


    ### Load initial parameter data into the catalog
    dagrunner.catalog.save(data_name='a',data_obj=10)
    dagrunner.catalog.save(data_name='aa',data_obj=10)
    dagrunner.catalog.save(data_name='bb',data_obj=10)
    dagrunner.catalog.save(data_name='cc',data_obj=10)
    dagrunner.catalog.save(data_name='f',data_obj=10)
    ### Scheduler function testing
    # ### Initialize Kahn work queue
    # dagrunner.scheduler.init_job_queue_with_kahn()
    # ### Using the Kahn algorithm to return the generator
    # run_list = dagrunner.scheduler.dispath_algorithm_with_kahn()
    # print(run_list,'--------------------------')
    # for i in run_list:
    #     print(i)
    #     ray_result = dagrunner.execute_algorithm_with_ray(run_list=i)
    #     print('==========',ray_result)


    ### Formal operation interface
    start_time = time.time()
    result = dagrunner.execute()
    end_time = time.time()
    print('cost time is {}'.format(end_time - start_time))
    print(result)
    print(dagrunner.catalog.load(data_name='d1'))


