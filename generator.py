# -*- coding: utf-8 -*-

from __future__ import print_function, division  # 确保兼容 Python 2 和 3 的 print 函数和除法行为
import json
import random
import time
import socket
from datetime import datetime

# 模拟用户和商品数据
user_ids = ["user_{}".format(i) for i in range(1, 101)]
product_ids = ["product_{}".format(i) for i in range(1, 15)]
actions = ["browse", "add_to_cart", "purchase"]

def generate_order_data():
    """生成一个订单数据的 JSON 对象"""
    order_id = "order_{}".format(random.randint(1000, 9999))
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    quantity = random.randint(1, 5)
    price = round(random.uniform(10.0, 500.0), 2)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    order_data = {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "total": round(quantity * price, 2),
        "timestamp": timestamp,
        "action": "purchase"
    }

    return order_data

def generate_user_behavior_data():
    """生成一个用户行为数据的 JSON 对象"""
    user_id = random.choice(user_ids)
    product_id = random.choice(product_ids)
    action = random.choice(actions)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    behavior_data = {
        "user_id": user_id,
        "product_id": product_id,
        "action": action,
        "timestamp": timestamp
    }

    return behavior_data

def start_server(host='127.0.0.1', port=9999):
    """启动服务器，将数据输出到指定的端口"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Server started at {host}:{port}. Waiting for connections...")

    conn, addr = server_socket.accept()
    print(f"Connection established with {addr}")

    try:
        while True:
            # 随机选择生成订单数据或用户行为数据
            if random.choice([True, False]):
                data = generate_order_data()
            else:
                data = generate_user_behavior_data()

            # 将数据转换为 JSON 格式
            json_data = json.dumps(data, ensure_ascii=False)

            # 发送数据
            conn.sendall((json_data + "\n").encode('utf-8'))

            # 随机等待一段时间，模拟数据生成的间隔
            time.sleep(random.uniform(0.05, 0.20))
    except KeyboardInterrupt:
        print("Server stopped.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    start_server()
