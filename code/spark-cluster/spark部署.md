### 第一步：准备工作 

1. **安装 Docker Desktop**：
   这是必需的核心工具。请从 Docker 官网下载并安装它。安装完成后，启动Docker Desktop，并确保它在后台正常运行。
   - **官网地址**: [https://www.docker.com/products/docker-desktop/](https://www.google.com/url?sa=E&q=https%3A%2F%2Fwww.docker.com%2Fproducts%2Fdocker-desktop%2F)
2. **（可选）调整Docker资源**：
   Docker Desktop默认会限制其能使用的CPU和内存。对于Spark，建议适当增加资源。点击顶部菜单栏的鲸鱼图标 -> Settings (或 Preferences) -> Resources。建议至少分配 **4-6个CPU核心** 和 **6-8 GB内存**。

### 第二步：获取Spark-cluster 文件夹

```
spark-cluster/
|-- docker-compose.yml /
├── hadoop-conf/
|   |-- core-site.xml      # Hadoop 核心配置文件
|   `-- hdfs-site.xml      # HDFS 特定配置文件
├── hadoop-data/
│   ├── datanode/          # 存储 HDFS DataNode 服务的实际数据块
│   └── namenode/          # 存储 HDFS NameNode 服务的元数据
└── spark-events/          # Spark 事件日志持久化目录
```

### 第三步：启动并验证集群

1.**启动集群**：

```
docker-compose up -d
```

2.**验证Spark Web UI**：

访问：**http://localhost:8080**：确认 Alive Workers 2

3.**验证HDFS UI** 

访问：**http://localhost:9870**: 确认 Live Nodes 1

