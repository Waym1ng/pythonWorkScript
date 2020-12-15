import redis
import time
import multiprocessing
import time
import os
import random
import json

redisPool = redis.ConnectionPool(host='localhost', port=6379, db=2)
client = redis.Redis(connection_pool=redisPool)

def insert_redis_queue():
    # 顺序插入五条数据到redis队列，sort参数是用来验证弹出的顺序
    while True:
        num = 0
        for i in range(0, 100):
            num = num + 1
            # params info
            params_dict = {"name": f"test {num}", "sort": num}

            client.rpush("test", json.dumps(params_dict))

        # 查看目标队列数据
        result = client.lrange("test", 0, 100)
        print(result)

        time.sleep(10)


def test1(msg):
    t_start = time.time()
    print("%s开始执行，进程号为%d" % (msg, os.getpid()))
    time.sleep(random.random() * 2)
    t_stop = time.time()
    print("%s执行完成，耗时%.2f" % (msg, t_stop - t_start))


def start():
    while True:
        number = client.llen('test')
        print("现在的队列任务 条数是 ", number)
        p = 100
        if number > p - 1:
            print("-----start-----")
            a = []
            for i in range(p):
                result = client.lpop("test")
                a.append(result)
            print("每10条读取一次", a)
            po = multiprocessing.Pool(p)
            for i in range(0, p):
                # Pool().apply_async(要调用的目标,(传递给目标的参数元祖,))
                # 每次循环将会用空闲出来的子进程去调用目标
                po.apply_async(test1, (a[i],))
            po.close()  # 关闭进程池，关闭后po不再接收新的请求
            po.join()  # 等待po中所有子进程执行完成，必须放在close语句之后
            print("-----end-----")
            time.sleep(2)
        elif number < p and number > 0:
            print("-----start-----")
            a = []
            for i in range(number):
                a = []
                result = client.lpop("test")
                a.append(result)
            print("小于10条的 读取一次 ", a)
            po = multiprocessing.Pool(number)
            for i in a:
                # Pool().apply_async(要调用的目标,(传递给目标的参数元祖,))
                # 每次循环将会用空闲出来的子进程去调用目标
                po.apply_async(test1, (a,))

            po.close()  # 关闭进程池，关闭后po不再接收新的请求
            po.join()  # 等待po中所有子进程执行完成，必须放在close语句之后
            print("-----end-----")
            time.sleep(2)
        elif number == 0:
            print("没有任务需要处理")
            time.sleep(2)
        else:
            time.sleep(2)

if __name__ == '__main__':
    # insert_redis_queue()
    start()