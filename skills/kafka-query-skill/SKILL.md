# Kafka 数据查询技能

## 简介

本技能允许你以灵活的方式查询 Kafka 主题中的数据。支持以下查询维度：

- 按时间范围（起始时间、结束时间）
- 按分区号
- 按消息 key
- 按偏移量范围
- 限制返回消息条数

## 使用前提

- 已安装 Python 3.8+ 和依赖库：`kafka-python`、`python-dateutil`、`pyyaml`
- 拥有可访问的 Kafka 集群连接信息（broker 地址、认证信息等）
- 配置文件位于 `~/.kafka-skill/config.yaml` 或通过环境变量指定

## 配置文件示例 (`~/.kafka-skill/config.yaml`)

```yaml
bootstrap_servers: "localhost:9092"
security_protocol: "PLAINTEXT" # 或 SASL_SSL
sasl_mechanism: "PLAIN"
sasl_plain_username: "user"
sasl_plain_password: "password"
# 如果使用 SSL，可配置 ssl_cafile, ssl_certfile, ssl_keyfile
```
