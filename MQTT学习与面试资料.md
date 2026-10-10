# MQTT 学习与面试资料

> 面向后端/物联网方向工程师，覆盖协议基础、报文细节、MQTT 5.0 新特性、运维实战与面试题库。  
> 建议阅读顺序：第 1~~4 章（打基础）→ 第 5~~7 章（进阶细节）→ 第 8 章（横向对比与选型）→ 第 9 章（面试题）。

---

## 目录

- [1. 基础概念](#1-基础概念)
- [2. MQTT 报文结构（最核心的细节）](#2-mqtt-报文结构最核心的细节)
- [3. QoS 三个等级与状态机](#3-qos-三个等级与状态机)
- [4. 关键机制详解](#4-关键机制详解)
- [5. MQTT 5.0 新特性](#5-mqtt-50-新特性)
- [6. 工程实战：Broker、客户端、ACL](#6-工程实战broker客户端acl)
- [7. 运维与排障](#7-运维与排障)
- [8. 横向对比与选型](#8-横向对比与选型)
- [9. 面试题库（含答案）](#9-面试题库含答案)
- [附录：速查表](#附录速查表)

---

## 1. 基础概念

### 1.1 什么是 MQTT

MQTT（**Message Queuing Telemetry Transport**，消息队列遥测传输）是 IBM 于 1999 年发布的一个**面向物联网的轻量级发布/订阅消息传输协议**。它工作在 TCP/IP 之上（也支持 WebSocket），核心目标是：**在受限设备、低带宽、高延迟、不可靠网络上，实现低功耗、低开销的设备到服务端双向通信**。

它的三个定位关键词：

| 关键词                | 含义                                       |
| ------------------ | ---------------------------------------- |
| **Pub/Sub（发布/订阅）** | 生产者和消费者**完全解耦**，生产者不知道消费者是谁、是否存在         |
| **Broker（代理服务器）**  | 消息中间件节点，负责接收、存储、按 Topic 分发               |
| **极简协议**           | 最小报文 2 字节，协议本身只有 ~40 页规范，无强制加密/无事务/无集群协议 |

### 1.2 版本演进（面试常问：说清版本差异）

| 版本                       | 时间                                   | 状态                                                 | 关键变化                                                |
| ------------------------ | ------------------------------------ | -------------------------------------------------- | --------------------------------------------------- |
| **MQTT 3.1**             | 2010（IBM 内部叫 MQ Telemetry Transport） | 已废弃                                                | 协议名限制为 6 字节，不支持 `WILL` 等                            |
| **MQTT 3.1.1**（OASIS 标准） | 2014-03                              | **最广泛部署**，绝大多数设备/库默认                               | 报文极简化，去掉冗余字段；`WILL` 改名 `Will Message`；协议名放宽为 `MQTT` |
| **MQTT 5.0**（OASIS 标准）   | 2019-03                              | 主流云厂商（Azure IoT / AWS IoT / EMQX 5 / HiveMQ 5）均已支持 | 原因码体系、属性字段、共享订阅标准化、请求-响应模式、限流                       |

> **协议名区别（易错）**：3.1 的协议名是 `MQIsdp`，3.1.1 及以后是 `MQTT`。服务端应同时接受两者做版本协商（CONNACK 返回 `0x04` 表示不支持协议版本）。

### 1.3 术语速查表

| 术语                              | 中文      | 说明                                           |
| ------------------------------- | ------- | -------------------------------------------- |
| **Client**                      | 客户端     | 连接 Broker 的设备或应用，可以是发布者也可以是订阅者               |
| **Broker**                      | 服务器/代理  | 中枢节点，接入所有客户端                                 |
| **Publish / Subscribe**         | 发布 / 订阅 | 发布是单向的，订阅是"登记兴趣"                             |
| **Topic**                       | 主题      | 消息的寻址路径，UTF-8 字符串，`/` 分层，支持通配符               |
| **Filter**                      | 主题过滤器   | 订阅时用的表达式，可含通配符                               |
| **Message ID**                  | 报文标识符   | QoS>0 时用于去重/状态跟踪，2~65535，**一个连接内不可复用直至确认完成** |
| **Keep Alive**                  | 保活周期    | 客户端声明的连接保活秒数                                 |
| **Retain**                      | 保留标志    | 新订阅者上线时立刻收到该 Topic 的最后一条保留消息                 |
| **Will Message**                | 遗嘱消息    | 客户端异常断开时由 Broker 代发的消息                       |
| **Clean Session / Clean Start** | 清理会话    | 是否清除 Broker 侧保存的会话与订阅                        |
| **Keep Alive 超时**               | —       | 1.5 倍 Keep Alive 无任何报文则服务端判定掉线并触发遗嘱          |

### 1.4 通信模型图（务必能默画出来）

```
        ┌──────────────┐   PUBLISH    ┌──────────┐   PUBLISH    ┌──────────────┐
        │  发布者 Client │ ───────────> │  Broker  │ ───────────> │ 订阅者 Client │
        │  (传感器/服务) │              │  (EMQX)  │              │ (消费方/告警) │
        └──────────────┘               └──────────┘              └──────────────┘
              │                              │  ↑
              └───────── SUBSCRIBE ──────────┘  │
           （订阅者只声明"我要哪些 Topic"，       │
             不知道发布者是谁，也不知道有谁在订阅）  └────── 遗嘱消息（Client → Broker）
```

**面试高频追问**：为什么 Pub/Sub 要引入 Broker 而不是客户端直连？  
答案：Broker 做了三件事 —— ① **解耦**（生产者和消费者互不知道对方）；② **扇出/广播**（一条消息可同时投递给 N 个订阅者，Client 直连需要 N 条连接）；③ **削峰与持久化**（离线订阅者重连后可补消息）。

---

## 2. MQTT 报文结构（最核心的细节）

**这是面试考察最集中的地方。** MQTT 报文 = **固定头（Fixed Header） + 可变头（Variable Header） + 载荷（Payload）**。固定头必须存在，另两部分可选。

### 2.1 固定头（Fixed Header）—— 所有报文都有，至少 2 字节

```
┌────────┬─────────────┬───────────────────────────┐
│ Byte 1 │  Byte 2-5   │      可选 Payload          │
│类型(4) │剩余长度(1-4) │                           │
│标志(4) │             │                           │
└────────┴─────────────┴───────────────────────────┘
```

**Byte 1：高 4 位 = 报文类型（0~15），低 4 位 = 标志位（各报文不同）**

| 值  | 类型          | 方向        | 标志位含义                                 |
| -- | ----------- | --------- | ------------------------------------- |
| 1  | CONNECT     | C→S       | **0**（必须为 0）                          |
| 2  | CONNACK     | S→C       | 0                                     |
| 3  | PUBLISH     | C→S / S→C | **bit3=DUP, bit2-1=QoS, bit0=RETAIN** |
| 4  | PUBACK      | 双向        | 0                                     |
| 5  | PUBREC      | 双向        | 0                                     |
| 6  | PUBREL      | 双向        | **bit1-0 = 0b0010**（固定为 2）            |
| 7  | PUBCOMP     | 双向        | 0                                     |
| 8  | SUBSCRIBE   | C→S       | **bit1-0 = 0b0010**（固定为 2）            |
| 9  | SUBACK      | S→C       | 0                                     |
| 10 | UNSUBSCRIBE | C→S       | bit1-0 = 0b0010（固定 2）                 |
| 11 | UNSUBACK    | S→C       | 0                                     |
| 12 | PINGREQ     | C→S       | 0                                     |
| 13 | PINGRESP    | S→C       | 0                                     |
| 14 | DISCONNECT  | 双向        | MQTT 5.0 低 4 位有意义（bit0=Will 消息）       |
| 15 | AUTH        | 双向        | MQTT 5.0 **新增**，用于增强认证                |

**PUBLISH 标志位拆解（重点）**：

```
Byte1 = [3] DUP  [2][1] QoS  [0] RETAIN
          │
          └── 重复投递标志：1 = 本报文是对之前一条 PUBLISH 的重发
              注意 QoS=0 时 DUP **必须**为 0；MQTT5 中 DUP 可用于指示
              "消息是否因服务端未确认而重发"，语义更精确
```

**Byte 2~5：剩余长度（Remaining Length）—— 变长整数**

- 编码方式：每字节低 7 位存数据（0~127），**最高位是继续标志**，最多 4 字节。
- 取值范围：`0 ~ 268,435,455`（约 256 MB）。

| 字节数 | 表示的字节数范围                | 十六进制示例                            |
| --- | ----------------------- | --------------------------------- |
| 1   | 0 ~ 127                 | `0x00`~`0x7F`                     |
| 2   | 128 ~ 16,383            | `0x80 0x01` = 128                 |
| 3   | 16,384 ~ 2,097,151      | `0x81 0x80 0x00` = 16384          |
| 4   | 2,097,152 ~ 268,435,455 | `0xFF 0xFF 0xFF 0x7F` = 268435455 |

> **手算示例（面试可能让你算）**：`0xD6 0x02` → 第一个字节 0xD6 = 1101 0110，最高位是 1（继续），取低 7 位 `0x56` = 86；第二个字节 0x02，最高位 0，取 `0x02` = 2 → 结果 = 86 + 2 × 128 = **342**。

### 2.2 CONNECT 报文（可变头字段顺序必须背出来）

```
Variable Header（顺序固定，不可乱）：
┌────────────────────┬──────────────────────────┐
│ Protocol Name      │ "MQTT" (0x00 0x04 'M'..'T') │
│ Protocol Level     │ 0x04(3.1.1) / 0x05(5.0)  │
│ Connect Flags      │ bit7 User Name           │
│                    │ bit6 Password            │
│                    │ bit5 Will Retain         │
│                    │ bit4~3 Will QoS          │
│                    │ bit2 Will Flag           │
│                    │ bit1 Clean Session/Start │
│                    │ bit0 reserved = 0        │
├────────────────────┼──────────────────────────┤
│ Keep Alive (2B)    │ 心跳周期，0 = 不启用      │
├────────────────────┼──────────────────────────┤
│ [Payload]          │ ClientID / Will Topic    │
│                    │ Will Message / Username  │
│                    │ / Password（按标志位决定）│
└────────────────────┴──────────────────────────┘
```

**关键点**：

- **ClientID 长度 1~23 字节**（MQTT 3.1.1）；5.0 由服务端分配（`Assigned Client Identifier`）。
- **MQTT 5.0 的 ClientID 可以为空**（长度为 0），此时服务端必须分配 ID 并在 CONNACK 里返回 `0x12 Assigned Client Identifier`。
- `Clean Session=1` 时，服务端**不保存**会话与订阅；重连后按"全新连接"处理。
- 3.1.1 中 `Will QoS` 与 `Will Retain` 只有在 `Will Flag=1` 时才有意义（否则必须为 0）。
- 用户名/密码**明文传输**（默认端口 1883），所以生产环境基本都上 TLS（8883）。

### 2.3 CONNACK 报文

```
Byte1: 0x20
Byte2: 剩余长度（=2 或 MQTT5 可能 >2）
Byte3: 连接确认标志 Session Present（0/1）
Byte4: 返回码 Reason Code
```

**返回码表（背下来）**：

| 返回码  | 名称                                                 | 含义          |
| ---- | -------------------------------------------------- | ----------- |
| 0x00 | Connection Accepted                                | 连接成功        |
| 0x01 | Connection Refused - unacceptable protocol version | 协议版本不支持     |
| 0x02 | Connection Refused - identifier rejected           | ClientID 非法 |
| 0x03 | Connection Refused - Server unavailable            | 服务端不可用      |
| 0x04 | Connection Refused - bad user name or password     | 账号密码错误      |
| 0x05 | Connection Refused - not authorized                | 未授权（ACL 拒绝） |

> **面试高频**：CONNACK 只有 2 字节，**没有可读的字符串错误信息**。所以排障时要在 Broker 侧查日志，客户端只能看到 `Connection refused: Not authorized` 这类固定文案。

### 2.4 PUBLISH 报文

```
Fixed Header：类型 3 + DUP/QoS/RETAIN + 剩余长度
Variable Header：
   ├ Topic Name（UTF-8，含长度字段）
   ├ [Message ID（2B）] —— QoS>0 才有
   └ [Properties] —— 仅 MQTT 5.0
Payload：业务数据，剩余长度减去前面所有部分
```

### 2.5 ACK 类报文（四组）

| 报文          | 方向           | 可变头                       | 作用              |
| ----------- | ------------ | ------------------------- | --------------- |
| **PUBACK**  | 双向           | Message ID (+ 5.0 原因码/属性) | QoS1 确认         |
| **PUBREC**  | 双向           | Message ID                | QoS2 第一阶段"收到"确认 |
| **PUBREL**  | C→S（固定 0x62） | Message ID                | QoS2 第二阶段"请求释放" |
| **PUBCOMP** | 双向           | Message ID                | QoS2 完成确认       |

> 3.1.1 里 PUBACK/PUBREC/PUBREL/PUBCOMP 都只有 2 字节可变头（纯 Message ID）。MQTT 5.0 允许携带原因码（用于告知"对方我拒绝/配额超限"）和属性。

---

## 3. QoS 三个等级与状态机

### 3.1 一句话对比

| QoS   | 名称                 | 语义               | 开销     | 典型场景                 |
| ----- | ------------------ | ---------------- | ------ | -------------------- |
| **0** | 最多一次 At most once  | **可能丢**，不会重复     | 1 个报文  | 高频传感器遥测、指标上报，丢一两条无所谓 |
| **1** | 至少一次 At least once | **不会丢**，**可能重复** | 4 个报文  | 设备控制指令、订单状态、告警通知     |
| **2** | 恰好一次 Exactly once  | 不丢不重             | 4+ 个报文 | 计费、支付、IM 消息、状态机严格场景  |

### 3.2 QoS 0 流程（最容易）

```
Client ──PUBLISH(qos=0)──> Broker ──PUBLISH(qos=0)──> Subscriber
```

没有 ACK，没有任何重传。**在 TCP 上它也不会丢包**（TCP 保证送达），但如果连接在传输途中断开、Broker 尚未持久化，就真的丢了。所以 QoS 0 的"可能丢"指的是**应用层不保证**。

### 3.3 QoS 1 流程（会有重复消费）

```
Client ──PUBLISH(qos=1, mid=1)──> Broker
Client <──────PUBACK(mid=1)────── Broker        ← 至少这一步

Broker ──PUBLISH(qos=1, mid=1)──> Subscriber
Broker <─────PUBACK(mid=1)───── Subscriber
```

- 超时未收到 PUBACK → 重发 PUBLISH，**DUP 位置 1**。
- QoS 1 **不保证"只投一次"**，所以消费者必须**幂等**（用 Message ID 去重、或业务唯一键）。

### 3.4 QoS 2 流程（四次握手，真正的不重不丢）

```
Client ──PUBLISH(qos=2, mid=1)──> Broker     ① 发布
Client <────PUBREC(mid=1)───── Broker          ② 收到确认（Broker 落库后）

Client ──PUBREL(mid=1)───────> Broker          ③ 释放
Client <───PUBCOMP(mid=1)──── Broker          ④ 完成
```

Broker 侧对 QoS2 消息的状态机（**面试爱问**）：

| 状态             | 收到 PUBLISH     | 收到 PUBREL            | 收到 PUBCOMP |
| -------------- | -------------- | -------------------- | ---------- |
| **等待 PUBREC**  | 存消息 → 回 PUBREC | —                    | —          |
| **等待 PUBCOMP** | 丢弃重复的 PUBLISH  | **删除消息** → 回 PUBCOMP | —          |
| **消息已删除**      | 丢弃             | 回 PUBCOMP            | 回 PUBCOMP  |

**为什么 QoS 2 能保证不重不丢**：Broker 收到 PUBLISH 后**先把消息存起来**，直到收到 PUBREL 才真正投递/删除。中间的重复 PUBLISH 会被状态机识别并丢弃。

### 3.5 QoS 决策树（面试答题模板）

```
数据丢失是否可接受？
├─ 可接受（遥测/指标）→ QoS 0（最省带宽）
└─ 不可接受
   └─ 能否容忍重复？
      ├─ 不能（计费/IM）→ QoS 2
      └─ 能（配合幂等）→ QoS 1 ← 90% 业务选它
```

**真实工程建议**：绝大多数场景用 **QoS 1 + 消费者幂等**，成本远低于 QoS 2，只有金融/计费类才上 QoS 2。

### 3.6 QoS 在"发布方→Broker"和"Broker→订阅方"是分开的

这是很多人搞混的点：**一个主题可以被发布者以 QoS 0 发布，但 Broker 向不同订阅者投递时用不同的 QoS**（由订阅时的 QoS 决定）。

| 订阅时 QoS ↓ / 发布时 QoS → | 0     | 1     | 2     |
| --------------------- | ----- | ----- | ----- |
| **0**                 | 0     | 0（降级） | 0（降级） |
| **1**                 | 0（降级） | 1     | 1（降级） |
| **2**                 | 0     | 1     | 2     |

**实际投递 QoS = min(发布 QoS, 订阅 QoS)**。

---

## 4. 关键机制详解

### 4.1 Topic 与通配符（必须动手记）

Topic 是 UTF-8 字符串，用 `/` 分层，长度可达 65535 字节。支持两种通配符**只能用在订阅过滤器里，不能用在发布 Topic 里**：

| 通配符     | 名称    | 匹配规则                        | 示例                                                                                           |
| ------- | ----- | --------------------------- | -------------------------------------------------------------------------------------------- |
| `+`     | 单层通配符 | 匹配**恰好一个**层级的任意字符串          | `sport/+/player1` 匹配 `sport/tennis/player1`、`sport/football/player1`，**不匹配** `sport/player1` |
| `#`     | 多层通配符 | 匹配**零个或多个**层级，**必须放在最后**    | `sport/#` 匹配 `sport`、`sport/tennis`、`sport/tennis/player1`                                   |
| `$SYS/` | 系统主题  | 以下划线开头的 Topic 保留给 Broker 自身 | `$SYS/broker/version`、`$SYS/broker/clients/connected`                                        |

**面试细节题**：

1. `sport/#` 能匹配 `sport` 本身吗？→ **能**（零层也匹配）。
2. `+` 能匹配空字符串吗？→ **能**，所以 `sport/+` 匹配 `sport/`。
3. `sport/#` 和 `sport/+` 匹配 `sport` 的区别？→ 只有 `#` 能。
4. 能发布到 `sport/#` 吗？→ **不能**，Broker 会直接判定为协议错误并断连。
5. `#` 和 `+` 能出现在 Topic 的中间吗？→ `#` 不能；`+` 可以（`a/+/b`）。

**共享订阅前缀 `$share/`**：`$share/Group1/sport/tennis` —— 同组客户端负载均衡，每条消息只给组内一个客户端。MQTT 5.0 已正式纳入规范（此前是 3.1.1 的社区扩展）。

### 4.2 Retain 保留消息

**机制**：带 `RETAIN=1` 标志的 PUBLISH，Broker 会**保存这条消息**（Topic + Payload + QoS）。之后任何客户端订阅该 Topic，Broker 会**立刻补发这条保留消息**。

**关键特性（面试常考）**：

| 问题                  | 答案                                                        |
| ------------------- | --------------------------------------------------------- |
| 保留消息在重启后还在吗？        | **在**。它被持久化到 Broker 的存储中（是否持久取决于配置）                       |
| 一个 Topic 能存多条保留消息吗？ | **不能，只能一条**。新消息**覆盖**旧的                                   |
| 删除保留消息怎么删？          | 发布一条**空 Payload**（零字节）的保留消息                               |
| 保留消息 + QoS 的关系？     | 两者独立。发布时 QoS 0 的保留消息也能被 QoS 1 的订阅者收到，但**投递 QoS 取 min**    |
| 订阅时带 Retain 标志吗？    | SUBSCRIBE 报文没有 Retain 位；**只有**「订阅瞬间补发」和「发布瞬间推给已在线订阅者」两种时机 |


**典型用途**：设备当前状态（在线/离线、最新温度）、MQTT 客户端（如 mosquitto_sub 启动即看到上次值）、Broker 自身版本。

### 4.3 Session（会话）持久化

**核心问题**：客户端断线时，Broker 要不要记住它？

| 配置 | Broker 侧行为 | 适用场景 |
|---|---|---|
| **Clean Session = 1**（或 5.0 的 Clean Start=1 且 Session Expiry=0） | 断线即**删除**会话和订阅；重连后不补消息 | 一次性上报、无状态传感器 |
| **Clean Session = 0**（5.0：Session Expiry Interval > 0） | Broker **保存**会话：订阅列表 + 未确认的 QoS>0 消息 + QoS1/2 状态机 | 移动端、物联网设备（MQTT 规范明确的设备场景） |

**MQTT 5.0 的重大改进**：把 3.1.1 里混在一起的两个概念拆开了——
- **Clean Start**：连接时**要不要清空**旧会话
- **Session Expiry Interval**（4 字节，单位秒）：断线后**会话保留多久**（0 = 断线即删，0xFFFFFFFF = 永久）

> **一句话记忆**：3.1.1 里 `Clean Session=0` 的"永久保留"效果，在 5.0 里要用 `Clean Start=1` + `Session Expiry=很大` 来表达。

**会话状态包含什么（面试答案）**：客户端订阅的所有 Topic Filter、服务端未发完的 QoS1/2 消息、已发出但未确认的 QoS1/2 消息及对应 Message ID。

### 4.4 Will Message 遗嘱消息

**机制**：CONNECT 时可携带一段遗嘱（Will Topic + Will Payload + Will QoS + Will Retain）。当发生以下情况时，Broker **代为发布**这条消息：

| 触发条件 | 说明 |
|---|---|
| 网络异常断开 | 拔网线、信号中断 |
| 心跳超时 | 超过 1.5 × Keep Alive 无报文 |
| 客户端主动 `DISCONNECT` | **不触发**（正常下线） |
| 客户端进程被 kill -9 | **会触发** |
| Broker 自身关闭 | 取决于实现，通常会发 |

**为什么需要**：设备（比如一个共享单车）突然离线，后台必须知道"车不见了"。

**MQTT 5.0 新增**：`Will Delay Interval` —— 遗嘱**延迟投递**，Broker 会先等这段时间，如果客户端在此期间**重连并清除遗嘱**（`Session Expiry > 0` 时发 DISCONNECT 原因码 0x04 前的处理），就取消发送。典型用途是给网络抖动留恢复窗口，避免设备瞬时断网就发告警。

**面试要点**：遗嘱也可以是 **Retain 消息**，这样新订阅者上线就能知道"这台设备已离线"。

### 4.5 Keep Alive / 心跳机制

- 客户端在 CONNECT 里声明 `Keep Alive`（秒），`0` 表示关闭。
- 客户端必须保证**不超过 Keep Alive 的时间**内发送**至少一个报文**（任何报文都行：PINGREQ、PUBLISH、或另一个方向的报文）。
- **服务端规则**：在 **1.5 × Keep Alive** 内没收到任何报文，就**判定客户端失联**，**主动断开连接**并**触发遗嘱消息**。
- 典型默认：60 秒 → 服务端 90 秒后踢掉。
- 心跳包 `PINGREQ`（`0xC0`，剩余长度 0）→ 服务端回 `PINGRESP`（`0xD0`）。

> **为什么不用 TCP 自带心跳**：TCP keepalive 默认 2 小时才探测，检测不出"半开连接"（NAT/防火墙静默丢包时 TCP 认为连接还在）。MQTT 的应用层心跳能在秒级发现断网。

### 4.6 MQTT over WebSocket（面试常问）

- 端口 **8083/8084**，路径 `/mqtt`。
- 用途：**浏览器里的 JS 客户端无法直接用原生 TCP**，只能走 WebSocket。MQTT.js 即基于此。
- 子协议：`mqtt`（明文）、`mqtts`（TLS）。
- 报文格式完全一致，只是承载在 WebSocket 帧里。
- 补充：MQTT 5.0 还定义了 **TCP 协议族之外的传输层**（`MQTT-SN` over UDP、`MQTT over QUIC`，RFC 9000），但生态成熟度远不如 TCP/WebSocket。

---

## 5. MQTT 5.0 新特性

### 5.1 为什么要有 MQTT 5.0（背景必答）

3.1.1 定于 2014 年，它诞生于"几乎不出错、几乎不超载"的理想网络。10 年后现实世界变了：需要**精细的错误诊断**、**限流防打爆**、**海量订阅**、**服务端主动推送（请求-响应）**。5.0 就是为此做的重构，且**完全向后兼容 3.1.1**。

### 5.2 返回码 → 原因码（Reason Code）

3.1.1 只有 5 个笼统的 CONNACK 返回码，排障基本靠猜。5.0 把所有 ACK 报文都扩展为**原因码 + 可选属性**，例如：

| 原因码 | 名称 | 场景 |
|---|---|---|
| 0x87 | Not authorized | 认证失败 |
| 0x8D | Keep Alive timeout | 心跳超时断开 |
| 0x8E | Session taken over | 会话被同 ClientID 的新连接顶掉 |
| 0x95 | Packet too large | 报文超过服务端 `Maximum Packet Size` |
| 0x97 | Quota exceeded | 超出配额 |
| 0x98 | Administrative action | 被管理员踢下线 |
| 0x9E | Shared Subscriptions not supported | 服务端不支持共享订阅 |
| 0xA0 | Maximum connect time | CONNECT 认证耗时超时 |

**价值**：客户端能据此做出正确决策（如 0x8E 就重置本地状态，0x95 就减小报文）。

### 5.3 Properties 属性机制

5.0 引入**属性表**（TLV 风格，挂在可变头和 payload 之间），实现"向后兼容的扩展"。**最重要的几个**：

| 属性 | ID | 作用 |
|---|---|---|
| **Payload Format Indicator** | 1 | 0 = 二进制未定格式，1 = UTF-8 字符（对 Webhook 类下游友好） |
| **Message Expiry Interval** | 2 | 消息有效期（秒），到期后 Broker 可丢弃 → **原生 TTL** |
| **Response Topic** | 8 | 配合 Correlation Data，实现**请求-响应模式** |
| **Correlation Data** | 9 | 请求与响应配对的关联 ID |
| **Subscription Identifier** | 11 | **订阅标识**：Broker 下发时带上 ID，客户端可据此知道这条消息是哪个订阅命中的（一个订阅匹配多个过滤器时尤其有用） |
| **Session Expiry Interval** | 17 | 会话保留时长 |
| **Will Delay Interval** | 24 | 遗嘱延迟投递 |
| **Receive Maximum** | 33 | **客户端/服务端能同时接收多少个 QoS>0 未确认报文（流控）** |
| **Topic Alias** | 35 | **报文别名**：用短 ID 代替长 Topic 名，**省带宽的关键特性** |
| **Maximum Packet Size** | 39 | 对方能接收的最大报文 → 客户端可提前裁剪 payload |
| **User Property** | 38 | 任意 key-value 用户自定义键值对 |

**Topic Alias 详解（面试加分点）**：连接建立后先发一条 `Topic Alias Maximum = N`，之后就可以用 `Topic Alias = 1` 发一个空 Topic 的 PUBLISH 来"绑定"短 ID，后续只用 1~N 号代替完整 Topic。对 `devices/sensor-001/temperature` 这种长 Topic，压缩率可达 80%+。

**Receive Maximum 详解**：3.1.1 允许客户端无限并发发 QoS>0 报文，服务端缓冲会被打爆。5.0 双方都声明"我最多同时收 N 条未确认"，超了服务端发 `0x93 Receive Maximum exceeded` 或阻塞。这是**标准的反打爆机制**。

### 5.4 请求-响应模式（Request/Response）

3.1.1 实现"客户端请求服务端"很别扭（只能自己建一个响应 Topic）。5.0 原生支持：

```
Client ──PUBLISH(topic="dev/cmd", payload="turn_on",
                Response Topic="dev/resp",
                Correlation Data="req-001")──> Broker
Client <──PUBLISH(topic="dev/resp", payload="ok",
                Correlation Data="req-001")── Broker
```
Broker 还能配置 **Response Topic（值替换）**，把 Topic 里的占位符如 `{id}` 替换成实际客户端 ID。

### 5.5 共享订阅正式标准化

`$share/{GroupName}/{filter}` 在 5.0 成为规范特性（并新增原因码 0x9E 表示服务端不支持）。组内负载均衡，每条消息只投给组内一个客户端。**注意**：共享订阅不保证顺序，且该 Topic 的消息不会像普通订阅那样被所有客户端收到。

### 5.6 其他

- **Server Reference**：服务端可以在 CONNACK 里返回另一个地址，让客户端**重定向**到集群另一节点。
- **AUTH 报文（类型 15）**：支持双向增强认证，如 Kerberos / SASL 挑战应答。
- **订阅标识 Subscription Identifier**：见 5.3。
- **Reason String**：任意 ACK 可携带人类可读的错误描述。

---

## 6. 工程实战：Broker、客户端、ACL

### 6.1 Broker 选型

| Broker | 语言 | 定位 | 适用场景 |
|---|---|---|---|
| **EMQX** | Erlang | 企业级 MQTT Broker，5.x 原生支持 MQTT 5 | **生产首选**。集群、认证、ACL、规则引擎、桥接、REST API 一应俱全 |
| **Mosquitto** | C | 极轻量（几百 KB 内存） | 边缘设备、测试环境、单机小规模 |
| **HiveMQ** | Java | 商业，企业级 | 已并入 HiveMQ CE/Enterprise |
| **VerneMQ** | Erlang | 高性能集群 | 大规模 |
| **NanoMQ** | C | 边缘/超轻 | 边缘网关 |
| **RabbitMQ** | Erlang | 通用 AMQP broker，MQTT 只是插件 | **不推荐当纯 MQTT Broker**，MQTT 插件功能受限 |
| **Mosquitto + Docker** | — | 快速起本地环境 | 联调 |

### 6.2 客户端库

| 语言 | 库 | 备注 |
|---|---|---|
| Java | **Eclipse Paho**（`org.eclipse.paho.client.mqttv3` / `v5`） | 官方，最通用，注意 `MqttAsyncClient` vs `MqttClient` 同步/异步两套 |
| C/C++ | Paho C | 嵌入式首选 |
| Python | **paho-mqtt**（1.6.x/2.x 两代 API 不兼容） | 生态常用；2.0 有 CallbackAPIVersion 破坏性变更 |
| .NET | **MQTTnet** | 高性能，4.x 版本变化较大 |
| Go | `eclipse/paho.mqtt.golang` / mochi-mqtt（服务端） | |
| JS | **MQTT.js** | 浏览器走 WebSocket |
| JavaScript 服务端 | `aedes` | Node.js 内嵌 Broker |
| 跨语言 CLI | **mosquitto_sub / mosquitto_pub** | 调试神器 |

### 6.3 一段可运行的最小示例（面试可白板写出来）

**Python 发布者：**
```python
import paho.mqtt.client as mqtt
import time, json

def on_connect(c, userdata, flags, rc):
    print(f"connected rc={rc}")
    c.publish("devices/sensor-001/temperature",
              json.dumps({"value": 25.6, "ts": int(time.time())}),
              qos=1, retain=True)

c = mqtt.Client(client_id="sensor-001")
c.username_pw_set("user", "pass")
c.tls_set(ca_certs="ca.pem")          # 生产必须 TLS
c.on_connect = on_connect
c.connect("broker.example.com", 8883, keepalive=60)
c.loop_start()
time.sleep(10)
c.loop_stop(); c.disconnect()
```

**Python 订阅者：**
```python
import paho.mqtt.client as mqtt

def on_message(c, u, msg):
    print(f"{msg.topic} [qos={msg.qos}, retain={msg.retain}] -> {msg.payload.decode()}")

def on_subscribe(c, u, mid, granted):   # granted 里的 QoS = 实际投递 QoS
    print("subscribed, granted qos:", granted)

c = mqtt.Client(client_id="sub-01")
c.on_message = on_message
c.on_subscribe = on_subscribe
c.connect("broker.example.com", 8883)
c.subscribe([("devices/+/temperature", 1), ("$share/g1/devices/#", 1)])
c.loop_forever()
```

### 6.4 安全要点（面试必问，答不全容易扣分）

| 要点 | 说明 |
|---|---|
| **传输加密** | 一律用 **TLS 8883**，废弃明文 1883。Paho 用 `tls_set(ca_certs=...)` 且**必须校验证书**（不要用 `tls_set()` 无参） |
| **账号认证** | CONNECT 报文里的 Username/Password **只是 Base64，不是加密**。MQTT 5.0 有 AUTH 报文可做挑战应答 |
| **ACL 授权** | **认证 ≠ 授权**。必须按客户端身份限制 Topic 读写范围，否则一个设备能订阅 `$SYS/#` 或发布他人 Topic |
| **ClientID 唯一性** | 5.0 允许服务端在 ClientID 为空时分配；4.0 之前的实现常有"**相同 ClientID 互踢**"的行为（原因码 0x8E Session taken over），共享订阅场景下必须用**不同 ClientID** |
| **不要把设备密钥硬编码** | 用动态注册（一机一密）或证书（mTLS） |
| **限流** | Broker 侧配置连接数、单客户端订阅数、消息速率、报文大小上限；5.0 靠 Receive Maximum 协商 |

**ACL 配置示例（EMQX 风格）：**
```
{allow, {username, "device-001"}, pubsub, ["devices/device-001/#"]}.
{deny,  {username, "device-001"}, pubsub, ["$SYS/#", "devices/+/+"]}.
{allow, {username, "service-x"}, pubsub, ["$share/group1/devices/#"]}.
```

---

## 7. 运维与排障

### 7.1 命令行工具速查

```bash
# 发布（注意 QoS 与 retain 标志）
mosquitto_pub -h broker -p 8883 -u user -P pass \
  -t 'devices/sensor-001/temperature' -q 1 -r -m '{"v":25.6}'

# 订阅：-v 打印 Topic，-i 显示订阅标识，-F 指定输出格式（可打印 QoS/retain）
mosquitto_sub -h broker -t 'devices/#' -q 1 -v -i

# 订阅共享组
mosquitto_sub -h broker -t '$share/g1/devices/#' -q 1 -v

# 只看 Retain 消息
mosquitto_sub -h broker -t 'devices/#' -R -n -W 3    # -R 只输出 retain，-n 退出时不打印
```

### 7.2 常见故障对照表

| 现象 | 可能原因 | 排查/修复 |
|---|---|---|
| CONNACK 返回 0x01 | 协议版本不匹配 | 客户端显式指定协议版本（Java 用 `MqttConnectOptions.setMqttVersion`） |
| CONNACK 返回 0x02 | ClientID 非法 | 3.1.1 要求 1~23 字符且为合法 UTF-8 |
| CONNACK 返回 0x04 | 账号密码错 | 检查服务端配置与客户端是否漏 `username_pw_set` |
| CONNACK 返回 0x05 | 未授权 | Broker 未开认证插件，或 ACL 拒绝 |
| CONNACK 0x05 但密码没错 | ClientID 未加进认证表 | EMQX 的 `authentication` + `authorization` 是两套配置 |
| 设备反复掉线 | 心跳超时 | 减小业务发送间隔或调大 Keep Alive（客户端）；确认无 NAT 超时（NAT 常见 5 分钟静默断开，需靠 MQTT 心跳保活，这正是设计目的） |
| 订阅收不到消息 | Topic 写错 / 通配符用错 / ACL 拒绝 / 发布方 QoS 与订阅 QoS 不匹配 | 订阅时加 `-v` 确认 Topic；用 `mosquitto_sub` 交叉验证 |
| 同一主题收到重复消息 | QoS1/2 的正常语义，或 TCP 重连后重发 | **消费者必须幂等**，不要试图靠 QoS0 解决 |
| 消息被"穿越"到历史 | Retain 消息 | 发布空 payload 清除；或订阅时忽略 retain 消息 |
| 改了代码但 Broker 不生效 | 会话持久化导致拿到旧订阅 | 客户端用 Clean Start=1 重连 |
| 报文超限被踢 | 报文 > `Maximum Packet Size` | 减小 payload，或用 Topic Alias |
| 5.0 客户端连 3.1.1 Broker | 版本协商失败 | 升级 Broker，或客户端显式指定 `MqttVersion.V_310` |

### 7.3 抓包与排障手段

```bash
# Wireshark 直接过滤 MQTT
tshark -i eth0 -Y "mqtt" -T fields -e mqtt.msgtype -e mqtt.topic -e mqtt.msg

# 调试 Broker
mosquitto -c mosquitto.conf -v      # -v 打印所有报文日志（定位必开）
```

**`mosquitto -v` 是排查 MQTT 问题性价比最高的手段**，能直接看到 CONNECT/CONNACK/PUBLISH 的每个报文。

---

## 8. 横向对比与选型

### 8.1 MQTT vs AMQP vs Kafka vs Redis Pub/Sub

| 维度 | **MQTT** | **AMQP** (RabbitMQ) | **Kafka** |
|---|---|---|---|
| 定位 | 设备侧 Pub/Sub 协议 | 企业级消息中间件 | 高吞吐日志/流式数据 |
| 传输 | TCP / WebSocket / UDP(MQTT-SN) | TCP | TCP |
| 开销 | **极小**（最小 2 字节） | 中 | 大 |
| QoS | 0/1/2（协议内建） | 丰富（confirm/return/requeue） | **无消息级 QoS**，靠 offset 重放 |
| 消息存储 | Broker 单点保留（有上限） | 队列持久化 | **强持久化 + 长期保留** |
| 吞吐 | 万级/s（单机 EMQX 十万级） | 万级/s | **百万级/s** |
| 广播/扇出 | **原生一对多** | 需 Exchange 绑定 | 消费者组内不广播，跨组才广播 |
| 移动端/弱网 | **极友好** | 一般 | 一般 |
| 事务 | 无 | 有 | 有（幂等生产者/事务） |

**选型结论（面试标准答案）**：
- **设备/物联网/移动端推送** → MQTT
- **企业内部异构系统、复杂路由、事务需求** → AMQP / RabbitMQ
- **日志、埋点、流式处理、消息回放** → Kafka
- **极轻量本地广播/缓存** → Redis Pub/Sub（**不持久、不可靠**）

### 8.2 MQTT vs HTTP

| | MQTT | HTTP |
|---|---|---|
| 方向 | 双向长连接，双工 | 请求-响应，一问一答 |
| 开销 | 连接复用，开销小 | 每次请求都有 header + 握手（HTTP/1.1） |
| 实时推送 | **强** | 需轮询 / SSE / WebSocket |
| QoS/离线消息 | **内建 3 级 + 遗嘱 + 保留 + 会话** | 无 |
| 适用 | 长连设备上行 + 下行指令 | 一次性请求、文件传输、开放 API |


### 8.3 MQTT 典型架构

```
[设备] --MQTT/TCP--> [EMQX 集群]
                      ├─ 认证/ACL (MySQL/JWT/静态)
                      ├─ 规则引擎 --> Webhook --> 业务服务
                      │            --> Kafka --> 数仓
                      │            --> 规则引擎写时序库 (InfluxDB/TDengine)
                      ├─ 数据持久化 (MySQL/EMQX RocksDB)
                      └─ $SYS 主题 --> 监控系统 (Prometheus/exporter)
```

**规则引擎（Rule Engine）是 EMQX 的杀手锏**：设备上行消息进来后，用 SQL 风格规则过滤/转换，然后转发到 HTTP/Kafka/MQTT/数据库，**应用层完全不用接管设备连接**，这是 MQTT 方案与传统"设备直连业务系统"的最大区别。

---

## 9. 面试题库（含答案）

### 9.1 基础概念（8 题）

**Q1. MQTT 是什么？为什么物联网场景用它而不是 HTTP？**
> MQTT 是发布/订阅型轻量消息协议。优势：① 报文小（最小 2 字节 header），省流量省电；② 双向长连接，设备可主动上行也可被下行指令，HTTP 要做上行必须轮询；③ 原生 3 级 QoS + 遗嘱 + 保留消息 + 会话持久化，弱网/断连语义完备；④ Broker 扇出广播，一对多天然。代价：不保证海量吞吐、不提供强持久化回放。

**Q2. MQTT 是 TCP 还是 UDP？**
> 官方传输是 TCP（1883/8883）。另有基于 WebSocket（8083/8084，浏览器唯一选择）、MQTT-SN over UDP、MQTT over QUIC 等补充传输。选择 TCP 是为了可靠有序；丢包由 QoS 在应用层补偿而非靠 UDP 重传。

**Q3. Pub/Sub 和传统消息队列（点对点）的区别？**
> P2P：一个消息只能被一个消费者消费，消费者组内竞争。Pub/Sub：生产者发到 Topic，**所有订阅该 Topic 的客户端都会收到一份**，生产者不知道消费者是谁。MQTT 是纯 Pub/Sub（消费组靠 `$share/` 扩展实现）。

**Q4. Broker 是什么？可以没有 Broker 吗？**
> Broker 是消息中枢，负责 Topic 路由、QoS 保障、会话存储、ACL。理论上可以无 Broker 直连（两个 Client 互发），但就失去了解耦、扇出、持久化，MQTT 也就没有意义了。

**Q5. ClientID 有什么作用？两个客户端用同一个 ClientID 会怎样？**
> ClientID 是 Broker 标识连接的键：用于保存会话、投递离线消息。**3.1.1 中相同 ClientID 的第二个连接会把第一个踢下线**（CONNACK 原因码 0x8E Session taken over）。MQTT 5.0 允许 ClientID 为空由服务端分配。共享订阅组内成员必须用不同 ClientID。

**Q6. MQTT 3.1.1 和 5.0 的主要区别？（必考）**
> ① 返回码 → 原因码体系，错误可诊断；② 新增 Properties 机制（TTL、Response Topic、Topic Alias、Subscription Identifier、User Property）；③ 会话概念拆分为 Clean Start + Session Expiry Interval；④ 新增 Receive Maximum 流控、Maximum Packet Size；⑤ 共享订阅标准化；⑥ 新增 AUTH 报文和 Server Reference 重定向。**向后兼容 3.1.1**。

**Q7. Topic 命名规范怎么设计？**
> 建议 `业务域/设备类型/设备ID/数据点` 或 `环境/区域/设备ID/属性`。要点：层级不宜过深（影响匹配和解析开销）；设备 ID 放在固定层级便于通配订阅；**给保留消息用固定 Topic**；避免中文和特殊字符（编码问题）。例：`factory/line1/press-001/temperature`。

**Q8. 消息顺序能保证吗？**
> **同一个 QoS 1 消息在传输过程中可能乱序**（因为重发机制：先发的可能因重传而后到）。MQTT 规范明确不保证顺序。需顺序时，Topic 中加单调递增序号或时间戳，消费端自行比对丢弃过期数据。

### 9.2 QoS 与可靠性（8 题）

**Q9. QoS 0/1/2 分别是什么？给一个必须用 QoS2 的场景和一个用 QoS0 的场景。**
> 0=最多一次（可能丢、不重复）；1=至少一次（不丢、可能重复）；2=恰好一次。QoS2 场景：计费扣款、IM 消息已读回执、跨系统分布式事务同步。QoS0 场景：设备高频上报温度/心跳/指标采样，丢几条无所谓。

**Q10. 为什么 QoS1 会重复？业务代码该怎么处理？**
> QoS1 只要未收到 PUBACK 就会重发 PUBLISH（DUP=1），而 Broker 可能**已经收到并投递了**前一次（ACK 丢失），于是重复。规范允许"最多一次 QoS1"，因此**消费者必须幂等**：用 Message ID 去重表、或用业务唯一键（设备 ID + 时间戳）做 upsert、或 Redis 分布式锁。

**Q11. QoS2 的四次报文是什么？Broker 为什么要先存后删？**
> PUBLISH → PUBREC → PUBREL → PUBCOMP。Broker 收到 PUBLISH 先落盘/入队，**收到 PUBREL 才真正处理或删除**，这样中间的重复 PUBLISH 能被状态机识别丢弃，从而保证不重。代价是 4 次往返 + 服务端存储成本。

**Q12. QoS 是端到端的还是跳一跳的？**
> **跳一跳（hop-by-hop）**。发布者→Broker 一段 QoS，Broker→每个订阅者各自一段。**实际投递 QoS = min(发布 QoS, 订阅 QoS)**。所以你能发布 QoS1 的主题，某个订阅者订阅 QoS0，最终它收到的是 QoS0。

**Q13. Broker 重启后 QoS1/2 的消息会丢吗？**
> 取决于 Broker 是否开启**持久化会话**。EMQX 等生产级 Broker 默认将 QoS1/2 消息与会话写入内置 RocksDB，重启可恢复；但内存型/轻量 Broker 或显式关闭持久化则会丢。**这是运维必查项**。QoS0 消息无论如何都不持久。

**Q14. Keep Alive 设为多少合适？服务端多久断开？**
> 常见 30~120 秒，默认 60。服务端判定阈值是 **1.5 × Keep Alive**（60 → 90 秒）。太短会因网络抖动误踢并触发遗嘱；太长会导致故障发现慢、无法计费和触发离线告警。NAT 场景尤其要靠这个心跳防止"半开连接"。

**Q15. 遗嘱消息什么时候会被发送？主动 DISCONNECT 会发吗？**
> 网络断开、心跳超时（1.5×KeepAlive）、进程被 kill、Broker 关闭时发送。**正常发 DISCONNECT 断开不发送**。遗嘱可以是 Retain 消息，新订阅者立刻知道设备已离线。MQTT 5.0 的 Will Delay Interval 还能延迟投递，给瞬时断网留恢复窗口。

**Q16. Clean Session=0 有什么副作用？**
> Broker 会为该客户端保存会话：订阅列表 + 未确认的 QoS1/2 消息，能耗和存储成本上升。**副作用是"幽灵订阅"**——如果客户端改了订阅又忘了用 Clean Start，旧订阅会残留，导致收到已取消 Topic 的消息。这也是排查"莫名收到消息"时的常见原因。

### 9.3 Topic 与 Retain（5 题）

**Q17. `+` 和 `#` 的区别？`sport/#` 能匹配 `sport` 吗？**
> `+` 匹配**恰好一个**层级（可为空串）；`#` 匹配**零个或多个**层级，且**必须放在过滤器最后**。`sport/#` 能匹配 `sport` 本身（零层匹配）。`sport/+` 不能匹配 `sport`，只能匹配 `sport/x`。

**Q18. 能发布带通配符的 Topic 吗？**
> **不能**。`sport/#` 作为发布 Topic 会被 Broker 判定为协议错误并**主动断连**。通配符只存在于订阅过滤器中。

**Q19. Retain 消息怎么工作？一条 Topic 能存几条？怎么删除？**
> 带 RETAIN=1 的 PUBLISH 会被 Broker 持久化，每个 Topic **只保留最新一条**（新的覆盖旧的）。新客户端订阅该 Topic 时立刻收到。新订阅者上线即收到，不代表是"实时推送"。**删除方式：向该 Topic 发布一条空 Payload（零字节）的保留消息**。可以用 `-R` 订阅参数过滤出保留消息。

**Q20. Retain 消息和 QoS 的关系？**
> 独立。Retain 是"要不要存"，QoS 是"怎么送"。QoS0 的 retain 消息也会被存，但投递 QoS 取 min。但注意：**Suback 里返回的授予 QoS 是订阅请求的 QoS**，实际投递按 min 计算。

**Q21. 什么是 `$SYS` Topic？**
> 以 `$` 开头的保留命名空间，给 Broker 自身指标用：`$SYS/broker/version`、`$SYS/broker/clients/connected`、`$SYS/broker/messages/received`。$share 也属于这类保留前缀。普通订阅者订阅 `$SYS/#` 需要显式授权。

### 9.4 场景设计题（高频，考察真实工程能力）

**Q22. 设计一个"共享单车"的 MQTT 方案，包含开锁指令与离线告警。**
> **Topic 设计**：
> - 上行：`bike/{bikeId}/status`（位置、电量、锁态）、`bike/{bikeId}/telemetry`
> - 下行：`bike/{bikeId}/cmd`（开锁/锁车）
> - 遗嘱（Retain）：`bike/{bikeId}/online`（payload = `0` 离线，`1` 在线）
> - **会话**：`Clean Session=0` + Session Expiry 长（比如 3600s），让断网期间的开锁指令在重连后补发
> - **QoS**：开锁指令 QoS1（**必须幂等**，用指令 ID 去重，避免重复开锁）；遥测 QoS0
> - **遗嘱延迟**：Will Delay Interval = 30s，防 GPS 信号瞬时丢失误报离线
> - **ACL**：车锁固件只能 `pub bike/{自己}/#` 且 `sub bike/{自己}/cmd`；运维后台走共享订阅 `$share/ops/bike/+/status`
> - **扇出**：一个后台微服务组用共享订阅分摊消息，避免每条状态都推给所有实例

**Q23. 一个设备连着 Broker，但 Web 后台看不到数据了，怎么排查？**
> 按顺序：① `mosquitto_sub -v -t '你的Topic' -q 1` 交叉验证 Broker 是否收到；② 若 Broker 也没收到 → 看设备端网络/是否被踢（查 `mosquitto -v` 日志里有没有 Keep Alive timeout 或 Session taken over）；③ 若 Broker 收到了 → 查 ACL（是不是订阅被拒）、规则引擎转发是否报错、Webhook 目标服务是否 5xx；④ 查会话残留（Clean Session 影响）导致收到重复或错乱；⑤ 检查是不是发布 QoS < 订阅 QoS 导致被降级；⑥ 用 `tshark -Y mqtt` 抓包看报文。

**Q24. 设备量级 100 万，怎么设计 Broker 集群？**
> 用 **EMQX 集群**（节点间通过 Erlang 分布式 + 分片 Topic 路由），要点：
> - 客户端统一入口 VIP/LB，**同一 ClientID 固定路由到同一节点**（会话亲和），可用 `session` 路由算法
> - 节点无状态（会话持久化到 RocksDB/外部存储），支持滚动重启
> - 认证走独立服务（JWT/数据库），避免本地文件
> - 按 Topic 前缀做**分片**（如按设备号哈希），订阅建立时跨节点转发
> - 限流兜底：连接数、单设备订阅数、消息速率、`Receive Maximum`
> - 监控 `$SYS` + EMQX Dashboard，Prometheus exporter
> - 若还需要历史数据消费，做**规则引擎 → Kafka**，MQTT 本身不做长期存储

**Q25. MQTT 消息被"穿越"到新连接的设备上，怎么排查？**
> 几乎一定是 **Retain 消息**。检查发布时是否带了 `-r` / `retain=True`，以及客户端是否用 `msg.retain` 字段过滤。解决：订阅时忽略 `msg.retain == True` 的消息，或向该 Topic 发布空 payload 清除。

**Q26. 如何保证同一台设备的多个服务消费消息不互相干扰？**
> 三种方式：① **不同 ClientID + 相同普通 Topic**（各自都会收到一份，即广播式，适合"每个服务都要全量数据"）；② **共享订阅 `$share/groupX/topic`**（组内只给一个，适合"多个实例分摊工作量"）；③ **规则引擎转发到不同 Kafka Topic / 不同 HTTP 端点**（最干净，解耦最彻底，推荐）。

**Q27. 为什么你的设备刚重启就收到一堆旧的、已经过时的状态消息？**
> `Clean Session=0` 保留了会话和订阅，且 Broker 侧有 Retain 消息和未确认消息。重连后 Broker 补发。**这其实是正确行为**，但如果业务上不想要，把状态类 Topic 的 ClientID 改成不持久，或发布空 retain 清理，或消息里带时间戳让消费端丢弃过期数据。

### 9.5 追问型"坑题"

**Q28. 说"at least once"时，为什么不能同时保证"顺序"？**
> 因为 QoS1 的重传机制（未收到 PUBACK 就重发）会让先发的消息后到，Broker 侧可能存在多个未确认消息并发处理。要顺序就必须严格串行化（等 ACK 再发下一条），吞吐会下降。**MQTT 的选择是：保证不丢（至少一次），不保证顺序。**

**Q29. MQTT 能保证 Exactly-once 吗？**
> **QoS2 是"消息级 exactly once"，不是"事务级 exactly once"**。它保证消息不重复投递，但无法避免外部副作用的重复（比如"扣款请求"被处理两次但消息只到一次的场景，需要业务侧幂等 + 唯一键）。另外 QoS2 只在单个连接/会话内保证，Broker 重启、多节点路由仍需额外机制。

**Q30. MQTT 消息最大能有多大？为什么不直接发文件/大文件？**
> 剩余长度字段最大 4 字节 → 报文最大约 256 MB，但**实践中强烈不建议**：占用 TCP 缓冲、Broker 内存/磁盘、单条大消息会阻塞整个连接、超出 `Maximum Packet Size` 直接被拒（原因码 0x95）。**大文件应走 HTTP/FTPS 分片或对象存储直传，MQTT 只传 URL/进度。** 另外 MQTT 的 `In Topic` 中 UTF-8 字节数需在 65535 内。

**Q31. 为什么我订阅 QoS1，收到的是 QoS0？**
> 投递 QoS = min(发布 QoS, 订阅 QoS)。发布方用 QoS0 发布，订阅方订阅 QoS1 也只能得到 QoS0。要收到 QoS1，**发布方也必须用 ≥1 发布**。这是很多人踩的坑。

**Q32. 客户端频繁被踢下线（日志 `Session taken over` / `0x8E`）？**
> 同一 ClientID 有第二个连接建立（常见于设备端有重连逻辑 + 之前连接没被正确关闭，或者客户端代码双实例/多线程重复 connect）。修法：断线前**优雅发 DISCONNECT** 并等待；检查是否有多个进程/定时器在连；5.0 可用 ClientID 为空让服务端分配。

**Q33. MQTT 在 IoT 场景的已知短板/缺点？**（考察是否真做过）
> ① 无内置**历史数据留存/回放**（需 Kafka/InfluxDB 补）；② 单 Broker 有吞吐上限，极端规模需集群且会话亲和增加复杂度；③ 报文**无压缩**（5.0 只能靠减少 Topic 长度或用 `Payload Format Indicator` 提示下游解压）；④ 认证仅 CONNECT 时的静态凭据，设备批量轮换/一机一密需要额外机制；⑤ 缺少强一致的分布式事务；⑥ QoS2 实现复杂，很多 Broker/库实现不严谨，**实际生产用得少（多为 QoS1 + 幂等）**。

### 9.6 你可以反问面试官的高质量问题

1. 你们设备侧用 QoS 多少？重复消息的消费幂等是怎么做的？
2. 设备是长连接还是短连接？Keep Alive 设的多少？NAT 超时怎么处理？
3. 留存消息（Retain）用在了什么业务场景？
4. 离线指令积压（队列消息）怎么解决的？MQTT 本身不存队列，你们怎么补的？
5. 鉴权是设备级还是用户级？ACL 是按 Topic 精确控制还是前缀通配？

---

## 附录：速查表

### A. 默认端口

| 端口 | 协议 | 说明 |
|---|---|---|
| 1883 | MQTT over TCP | 明文，生产禁用 |
| 8883 | MQTT over TLS | **生产标准** |
| 8083 | MQTT over WebSocket | 明文 |
| 8084 | MQTT over WebSocket + TLS | |
| 9001 | MQTT over WebSocket（EMQX 默认） | EMQX 特有 |

### B. CONNACK 返回码（3.1.1）

| 码 | 含义 |
|---|---|
| 0 | 接受 |
| 1 | 协议版本不支持 |
| 2 | ClientID 被拒 |
| 3 | 服务端不可用 |
| 4 | 用户名或密码错 |
| 5 | 未授权 |

### C. 常用话题速查

| 目的 | 命令 |
|---|---|
| 快速收发测试 | `mosquitto_sub -h ip -t 'test' -v &` 然后 `mosquitto_pub -h ip -t 'test' -m 'hi'` |
| 打印所有系统指标 | `mosquitto_sub -h ip -t '$SYS/#' -v` |
| 查 Broker 版本 | `mosquitto_sub -h ip -t '$SYS/broker/version' -C 1` |
| 在线连接数 | `mosquitto_sub -h ip -t '$SYS/broker/clients/connected' -v` |
| 清掉某 Topic 的 retain | `mosquitto_pub -h ip -t 'topic' -r -n` |
| 共享订阅压测 | `mosquitto_pub -h ip -t '$share/g/test' -m x` + 多个 `mosquitto_sub -t '$share/g/test'` |

### D. 报文类型速查

```
0 保留   1 CONNECT    2 CONNACK   3 PUBLISH   4 PUBACK
5 PUBREC 6 PUBREL     7 PUBCOMP   8 SUBSCRIBE 9 SUBACK
10 UNSUBSCRIBE  11 UNSUBACK  12 PINGREQ  13 PINGRESP
14 DISCONNECT  15 AUTH (MQTT5)
```

### E. Java Paho 常见 API 对照

```java
MqttClient c = new MqttClient("ssl://host:8883", "clientId", new MemoryPersistence());
MqttConnectOptions o = new MqttConnectOptions();
o.setCleanSession(false);           // 5.0 用 setCleanStart(false) + setSessionExpiryInterval(3600)
o.setKeepAliveInterval(60);
o.setAutomaticReconnect(true);
o.setConnectionTimeout(30);
o.setUserName("u"); o.setPassword("p".toCharArray());
c.setCallback(new MqttCallbackExtended() {
    public void connectComplete(boolean r, String s) {
        c.subscribe("devices/#", 1, (t, g) -> {});   // 订阅必须放这里，session present=false 时要重订
    }
    public void messageArrived(String t, MqttMessage m) { /* m.getQos() m.isRetained() */ }
    public void deliveryComplete(IMqttToken tk) {}
    public void connectionLost(Throwable t) { /* 避免抛异常导致中断重连 */ }
});
c.connect(o);
```

> **坑点**：`MqttCallback` 的 `connectionLost` 里**抛异常会终止自动重连**，必须吃掉异常。以及订阅要放在 `connectComplete` 里并在 `sessionPresent == false` 时重新订阅，否则 Broker 重启后订阅丢失。

---

**文档版本**：v1.0 | 覆盖 MQTT 3.1.1 + 5.0 | 2026-10-10