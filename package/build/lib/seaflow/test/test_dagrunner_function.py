from seaflow.runners.dagrunner import DAGRunnerFunction,DAGRunnerNode
import time



dagrunner = DAGRunnerFunction()
### 注册目标函数到指定dagrunner控制器
# @dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_a():
    time.sleep(3)
    print('test_a')

test_a = dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)(test_a)

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_aa():
    time.sleep(3)
    print('test_aa')

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_b():
    time.sleep(3)
    print('test_b')

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_bb():
    time.sleep(3)
    print('test_bb')

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_c():
    time.sleep(3)
    print('test_c')

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_cc():
    time.sleep(3)
    print('test_cc')

@dagrunner.scheduler.register(controller_obj=dagrunner.scheduler)
def test_d():
    time.sleep(3)
    print('test_d')


### 注册目标函数依赖关系到指定cuber控制器
test_a >> test_b >> test_c >> test_d
test_aa >> test_b >> test_d
test_bb >> test_c >> test_d
test_cc >> test_d


### 展示DAG图情况
# dagrunner.scheduler.draw()


### 展示节点和边情况
print('~~~~~~',dagrunner.scheduler.get_graph_obj())
print('------',dagrunner.scheduler.show_nodes())
print('======',dagrunner.scheduler.show_edges())


### 执行各个节点函数
start_time = time.time()
result = dagrunner.execute()
print(result)
end_time = time.time()
print('cost time is {}'.format(end_time - start_time))


