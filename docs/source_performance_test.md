---
alias: Performance Test on AWS SQS Source Connector
---
## Background
During the performance test for the SQS source connector, we found its throughput was very low. The throughput of a pod is about 40/s, called `initialTPS.` For details, see [issue 484](https://github.com/streamnative/pulsar-io-sqs/issues/484).

To improve the throughput of a single pod, we have made some optimizations:
- Support pulling batch messages with a receive request.
- Start multiple consumer threads within a pod to consume.

## Test Plan
Based on the previous test, we change the `batchSizeOfOnceReceive` and `numberOfConsumers` options to observe what happens to throughput.

Desired result:
1. When we set the `batchSizeOfOnceReceive` to 10, the throughput is ten times greater than before.
2. When we set the `numberOfConsumers` to 10, the throughput increases linearly.
3. When we set both, the throughput of a single node can reach 10000/s.

## Operating environment
The test environment is the same as before #484.
## Test Senarios
### Scenario 1: Increase the batchSizeOfOnceReceive
Set the `numberOfConsumers` to 1 and `batchSizeOfOnceReceive` to 10. It shows a tenfold increase in Tps (The Tps = 400/s = initialTPS*10).

![image](https://user-images.githubusercontent.com/6015134/199689363-6ebc09c1-8e52-4c0d-9419-3ce3b5ca0eda.png)
```
2022-11-03T09:40:25,196+0000 [pulsar-timer-5-1] INFO  org.apache.pulsar.client.impl.ProducerStatsRecorderImpl - [test-queue-pulsar] [pulsar-108-31] Pending messages: 0 --- Publish throughput: 380.65 msg/s --- 0.37 Mbit/s --- Latency: med: 11.000 ms - 95pct: 15.000 ms - 99pct: 17.000 ms - 99.9pct: 59.000 ms - max: 79.000 ms --- BatchSize: med: 10.000 - 95pct: 10.000 - 99pct: 10.000 - 99.9pct: 10.000 - max: 10.000 --- MsgSize: med: 1280.000 bytes - 95pct: 1280.000 bytes - 99pct: 1280.000 bytes - 99.9pct: 1280.000 bytes - max: 1280.000 bytes --- Ack received rate: 380.65 ack/s --- Failed messages: 0 --- Pending messages: 0
2022-11-03T09:41:25,198+0000 [pulsar-timer-5-1] INFO  org.apache.pulsar.client.impl.ProducerStatsRecorderImpl - [test-queue-pulsar] [pulsar-108-31] Pending messages: 0 --- Publish throughput: 376.65 msg/s --- 0.37 Mbit/s --- Latency: med: 11.000 ms - 95pct: 15.000 ms - 99pct: 16.000 ms - 99.9pct: 39.000 ms - max: 76.000 ms --- BatchSize: med: 10.000 - 95pct: 10.000 - 99pct: 10.000 - 99.9pct: 10.000 - max: 10.000 --- MsgSize: med: 1280.000 bytes - 95pct: 1280.000 bytes - 99pct: 1280.000 bytes - 99.9pct: 1280.000 bytes - max: 1280.000 bytes --- Ack received rate: 376.65 ack/s --- Failed messages: 0 --- Pending messages: 0
```

### Scenario 2: Add the numberOfConsumers
In this scene, set the `numberOfConsumers` to 10 and `batchSizeOfOnceReceive` to1. The figure below shows that Tps increases linearly (The Tps = 400/s = initialTPS*10).
<img width="921" alt="image" src="https://user-images.githubusercontent.com/6015134/199722036-ed9783dd-c376-4356-99e5-4517d3a34b24.png">

### Scenario 3: Change both of them
Set `numberOfConsumers` to 10 and `batchSizeOfOnceReceive` to 10. We expect a 100-fold increase in Tps. The results are shown below (Tps=4000/s=100*`initialTPS`)
![image](https://user-images.githubusercontent.com/6015134/200255470-7eb20144-0b05-4604-a8fc-db2c880dc091.png)


### Scenario 4: Start multiple source Pods
The resources of the test environment are limited to us. Set `parallelism` to 2, set `numberOfConsumers` to 30, and set `batchSizeOfOnceReceive` to 10. The Tps eventually reach 20000/s.
![image](https://user-images.githubusercontent.com/6015134/200277829-23f9a2dc-e717-44d1-8169-18cfe1749c52.png)

You can see that the Tps of a single pod is around 10_000/s.
<img width="1709" alt="image" src="https://user-images.githubusercontent.com/6015134/200277460-54d7c8f6-b0ec-40eb-aa62-30d261b8e2bf.png">

The CPU was exhausted due to resource limitations in the test environment, so the relationship between Tps and consumer thread count is not completely linear.
![image](https://user-images.githubusercontent.com/6015134/200277562-680be200-3d69-4596-ac9e-9a20ba15cdc0.png)

## Summarize
From the above tests, it is concluded that the Tps can be significantly improved. Here are some suggestions for using it.
- If the expected Tps is less than 10_000/s, change the `batchSizeOfOnceReceive` and `numberOfConsumers` to improve the source performance.
- If the Tps exceeds 10_000/s, modify the `parallelism` to scale out and improve the Tps.
