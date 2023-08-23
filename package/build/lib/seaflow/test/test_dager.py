from seaflow.schedulers.dag import DAGer
from seaflow.nodes.node import Node
from seaflow.ios.io import LocalDataCatalog
import inspect



dager = DAGer()
### 注册目标函数到指定dager控制器
# @dager.register(controller_obj=dager)
def test_a(a):
    return a + 2
test_a = dager.register(controller_obj=dager)(test_a)

@dager.register(controller_obj=dager)
def test_aa(aa):
    return aa + 2

@dager.register(controller_obj=dager)
def test_b(b):
    return b + 2

@dager.register(controller_obj=dager)
def test_bb(bb):
    return bb + 2

@dager.register(controller_obj=dager)
def test_c(c):
    return c + 2

@dager.register(controller_obj=dager)
def test_cc(cc):
    return cc + 2

@dager.register(controller_obj=dager)
def test_d(d):
    return d + 2


### 注册目标函数依赖关系到指定cuber控制器
test_a >> test_b >> test_c >> test_d
test_aa >> test_b >> test_d
test_bb >> test_c >> test_d
test_cc >> test_d


### 测试node的call
def first_func(a,b):
    c = a + b
    return c
first_node = Node(func=first_func,inputs=['a','b'],outputs='c',name='first_func')
print('------>>>',first_node(100,10),first_node.run(100,10))
print(type(first_node)==Node)


### 展示节点和边情况
### 注册一个first_func，只加一个进入边，只是测试用
first_func= dager.register(controller_obj=dager)(first_node)
test_cc >> first_func >> test_d
print('~~~~~~',dager.get_graph_obj())
print('------',dager.show_nodes())
print('======',dager.show_edges())


### 展示DAG图情况
# dager.draw()


### 调用目标函数
tmp_func_a = dager.get_function_obj(function_name='test_aa')
tmp_func_b = dager.get_function_obj(function_name='first_func')
print(tmp_func_a(10))


### 调度器函数测试
### 初始化kahn工作队列
dager.init_job_queue_with_kahn()
### 使用kahn算法返回生成器
run_list = dager.dispath_algorithm_with_kahn()
print(run_list)
for i in run_list:
    print(i)


### 测试编码
### 获取函数输入
print(type(tmp_func_a),type(tmp_func_b))
tmp_varnames = tmp_func_a.__code__.co_varnames
print(tmp_varnames,type(tmp_varnames),tmp_varnames)
argspec = inspect.getfullargspec(tmp_func_a)
print(argspec.args)
print(tmp_func_b.inputs)
### 判断对象类型
print(inspect.isfunction(tmp_func_a),isinstance(tmp_func_b,Node))
### 设置缓存
cache_data = LocalDataCatalog()
cache_data.save(data_name='aa',data_obj=10)
print(cache_data.load(data_name='aa'))
### 从缓存中获取参数数据，并运行函数
print(tmp_func_a(cache_data.load(data_name='aa')))


