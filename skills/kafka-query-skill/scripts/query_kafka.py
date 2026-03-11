## 查询脚本 (scripts/query_kafka.py)

"""
Kafka 数据查询脚本
支持多种过滤条件，返回 JSON 格式结果。
"""

import argparse
import json
import sys
import os
import yaml
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from dateutil import parser as dtp
import time

#CONFIG_PATH = os.path.expanduser("~/.kafka-skill/config.yaml") # 从家目录下读取
CONFIG_PATH = os.path.abspath("./scripts/config.yaml")# 从家目录下读取

def load_config():
    """加载配置文件"""
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"配置文件不存在: {CONFIG_PATH}")
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def parse_time(time_str):
    """将时间字符串转为毫秒时间戳"""
    if time_str:
        dt = dtp.parse(time_str)
        return int(dt.timestamp() * 1000)
    return None

def query_kafka(args):
    config = load_config()
    consumer = KafkaConsumer(
        bootstrap_servers=config['bootstrap_servers'],
        security_protocol=config.get('security_protocol', 'PLAINTEXT'),
        # sasl_mechanism=config.get('sasl_mechanism'),
        # sasl_plain_username=config.get('sasl_plain_username'),
        # sasl_plain_password=config.get('sasl_plain_password'),
        # ssl_cafile=config.get('ssl_cafile'),
        # ssl_certfile=config.get('ssl_certfile'),
        # ssl_keyfile=config.get('ssl_keyfile'),
        enable_auto_commit=False,
        auto_offset_reset='earliest'  # 从最早开始，但我们会手动 seek
    )
    # 获取主题分区
    partitions = consumer.partitions_for_topic(args.topic)
    if not partitions:
        print(json.dumps({"error": f"Topic '{args.topic}' not found"}))
        return
    # 确定要查询的分区
    if args.partition is not None:
        if args.partition not in partitions:
            print(json.dumps({"error": f"Partition {args.partition} not in topic"}))
            return
        target_partitions = [args.partition]
    else:
        target_partitions = list(partitions)
    # 转换时间戳
    from_ts = parse_time(args.from_time) if args.from_time else None
    to_ts = parse_time(args.to_time) if args.to_time else None

    all_messages = []
    max_msgs = args.max_messages

    for part in target_partitions:
        tp = TopicPartition(args.topic, part)
        consumer.assign([tp])

        # 确定起始偏移量
        if args.from_offset is not None:
            start_offset = args.from_offset
        elif from_ts is not None:
            # 通过时间戳查找偏移量
            offsets = consumer.offsets_for_times({tp: from_ts})
            if offsets[tp] is None:
                continue  # 无数据
            start_offset = offsets[tp].offset
        else:
            # 默认从最早
            start_offset = consumer.beginning_offsets([tp])[tp]

        # 确定结束偏移量
        if args.to_offset is not None:
            end_offset = args.to_offset
        elif to_ts is not None:
            # 注意：offsets_for_times 返回的是第一个大于等于时间戳的偏移量
            # 我们要结束位置，可以取时间戳+1毫秒的偏移量
            offsets_end = consumer.offsets_for_times({tp: to_ts + 1})
            if offsets_end[tp] is None:
                # 如果没有消息大于 to_ts，则取最新偏移量
                end_offset = consumer.end_offsets([tp])[tp]
            else:
                end_offset = offsets_end[tp].offset
        else:
            # 默认到最新
            end_offset = consumer.end_offsets([tp])[tp]

        if start_offset >= end_offset:
            continue

        # 设置消费位置
        consumer.seek(tp, start_offset)

        # 添加超时参数
        timeout_seconds = 30  # 最大等待30秒
        start_time = time.time()

        # 消费消息
        print("loop1...")
        for msg in consumer: # 这个位置会卡住
            print("loop2...")
            # 检查是否超时
            if time.time() - start_time > timeout_seconds:
                print(json.dumps({"warning": f"Query timed out[{timeout_seconds}], partial results returned"}))
                break
            if len(all_messages) >= max_msgs:
                break
            if msg.offset >= end_offset:
                break

            # 按 key 过滤
            if args.key is not None:
                # key 可能是 bytes，解码为字符串比较
                key_str = msg.key.decode('utf-8') if msg.key else ''
                if args.key not in key_str:  # 支持子串匹配
                    continue

            # 解码 value（假设为 JSON 或字符串）
            try:
                value = json.loads(msg.value.decode('utf-8'))
            except:
                value = msg.value.decode('utf-8', errors='ignore')

            all_messages.append({
                "offset": msg.offset,
                "key": msg.key.decode('utf-8') if msg.key else None,
                "value": value,
                "timestamp": msg.timestamp,
                "partition": msg.partition
            })

    consumer.close()

    # 输出
    output = {
        "topic": args.topic,
        "messages": all_messages,
        "count": len(all_messages)
    }
    if args.output == 'json':
        print(json.dumps(output, indent=2))
    else:
        # 文本格式简单打印
        for msg in output['messages']:
            print(f"[{msg['partition']}:{msg['offset']}] key={msg['key']} time={msg['timestamp']} value={msg['value']}")

if __name__ == "__main__":
    print("start to parse args...")
    parser = argparse.ArgumentParser(description="Kafka 数据查询工具")
    parser.add_argument("--topic", required=True, help="Kafka topic 名称")
    parser.add_argument("--from-offset", type=int, help="起始偏移量")
    parser.add_argument("--to-offset", type=int, help="结束偏移量")
    parser.add_argument("--from-time", help="起始时间，格式 'YYYY-MM-DD HH:MM:SS'")
    parser.add_argument("--to-time", help="结束时间")
    parser.add_argument("--partition", type=int, help="分区号")
    parser.add_argument("--key", help="消息 key 过滤（子串匹配）")
    parser.add_argument("--max-messages", type=int, default=10, help="最大返回条数")
    parser.add_argument("--output", choices=['json', 'text'], default='json', help="输出格式")
    args = parser.parse_args()
    
    try:
        print("start to query...")
        query_kafka(args)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)