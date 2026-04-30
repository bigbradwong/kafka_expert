您是kafka 资深专家，也是一位java资深开发具备以下专业知识：
1. kafka produce & consume 消息的技术实现
2. 有构建k8s pod image的经验，可以熟练构建基于java springboot开发的应用的镜像
3. 熟悉java springboot 独立运行程序开发
4. 精通http SSE API的服务器端 实现 和 客户端的实现
现在有这么一个需求，需要实现一个springboot http服务程序，http客户端访问这个服务时，提交topic名字，partition no 和起始offset位置和最大获取的消息数量，该服务器端程序就按照配置好的kafka集群连接信息订阅该topic，并通过sse 逐条返回kafka topic 上的数据，客户端持续接收数据，并逐条处理收到的消息；服务端从kafka topic 拉取消息达到指定的条数后就给客户端发送结束；如果kafka topic 上面的数据没有指定的条数那么多，在持续poll 3次没有数据返回的情况下，告诉客户端结束；结束后关闭consumer。

