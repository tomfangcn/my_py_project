## 查询脚本 (scripts/query_kafka.py)

"""
Kafka 数据查询脚本
支持多种过滤条件，返回 JSON 格式结果。
python scripts/query_kafka.py --topic orders  --max-messages 2
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

    # 设置轮询超时（毫秒）
    poll_timeout_ms = 1000  # 每次 poll 最多等待1秒
    max_idle_seconds = 10   # 连续无消息的最长等待时间，超过即退出
    idle_start = None       # 记录空闲开始时间

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

        # 重置空闲计时器
        idle_start = None

        # 消费消息
        print("loop...")
        while True:
            # 检查是否达到最大消息数
            if len(all_messages) >= max_msgs:
                break
            
            # 使用 poll 获取一批消息
            msg_pack = consumer.poll(timeout_ms=1000, max_records=100)

            if not msg_pack:
                # 没有消息返回，可能是空闲
                if idle_start is None:
                    idle_start = time.time()
                elif time.time() - idle_start > max_idle_seconds:
                    # 连续空闲超时，退出循环（避免卡死）
                    print(f"Warning: No messages for {max_idle_seconds} seconds, stopping partition {part}", file=sys.stderr)
                    break
                continue
            else:
                # 有消息，重置空闲计时器
                idle_start = None

            # 处理返回的消息
            for tp, messages in msg_pack.items():
                for msg in messages:
                    if msg.offset >= end_offset:
                        break

                    # 按 key 过滤
                    if args.key is not None:
                        key_str = msg.key.decode('utf-8') if msg.key else ''
                        if args.key != key_str:
                            continue

                    # 解码 value
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

                    if len(all_messages) >= max_msgs:
                        break
                if len(all_messages) >= max_msgs:
                    break
            if len(all_messages) >= max_msgs:
                break

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