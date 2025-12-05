#!/bin/bash
# Spark分区策略实验运行脚本

set -e

echo "=========================================="
echo "Spark Partition Strategy Experiment - Simple"
echo "=========================================="

# 检查Docker是否可用
if ! command -v docker &> /dev/null; then
    echo "Error: Docker not found"
    exit 1
fi

# 检查Docker权限，如果失败则尝试使用sudo
if ! docker ps &> /dev/null; then
    echo "Warning: Docker permission denied, trying with sudo..."
    DOCKER_CMD="sudo docker"
else
    DOCKER_CMD="docker"
fi

# 检查Spark容器是否运行
if ! $DOCKER_CMD ps | grep -q spark-master; then
    echo "Error: Spark cluster is not running"
    echo "Please start docker-compose first: cd .. && docker-compose up -d"
    exit 1
fi

echo "✓ Spark cluster is running"

# 复制Python脚本到Spark容器
echo ""
echo "Step 1: Copying Python scripts to Spark container..."
$DOCKER_CMD exec spark-master mkdir -p /opt/spark/work
$DOCKER_CMD cp generate_data.py spark-master:/opt/spark/work/
$DOCKER_CMD cp custom_partitioner.py spark-master:/opt/spark/work/
$DOCKER_CMD cp partitioner_experiment.py spark-master:/opt/spark/work/
echo "✓ Scripts copied to /opt/spark/work/"

# 在Spark容器中使用spark-submit提交作业，运行实验并捕获输出
echo ""
echo "Step 2: Running experiment with spark-submit..."
echo "=========================================="

EXPERIMENT_OUTPUT=$($DOCKER_CMD exec spark-master bash -c "cd /opt/spark/work && /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --py-files custom_partitioner.py,generate_data.py \
    --conf spark.sql.shuffle.partitions=12 \
    --conf spark.executor.memory=2g \
    --conf spark.executor.cores=2 \
    --conf spark.driver.memory=1g \
    partitioner_experiment.py 2>&1")
EXPERIMENT_EXIT_CODE=$?

# 显示实验输出
echo "$EXPERIMENT_OUTPUT"

if [ $EXPERIMENT_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "⚠ Experiment exited with code $EXPERIMENT_EXIT_CODE"
    exit $EXPERIMENT_EXIT_CODE
fi

# 复制结果文件回本地
echo ""
echo "Step 3: Copying results from container..."
mkdir -p result

# 检查结果文件可能的位置（按优先级）
result_found=false
result_locations=(
    "/opt/spark/work/result/experiment_results.json"
    "/opt/spark/work/experiment_results.json"
    "/opt/spark/experiment_results.json"
)

for location in "${result_locations[@]}"; do
    if $DOCKER_CMD exec spark-master test -f "$location" 2>/dev/null; then
        $DOCKER_CMD cp "spark-master:$location" ./result/experiment_results.json
        echo "✓ experiment_results.json copied to result/"
        result_found=true
        break
    fi
done

if [ "$result_found" = false ]; then
    echo "⚠ experiment_results.json not found in container"
    echo "Checked locations:"
    for location in "${result_locations[@]}"; do
        echo "  - $location"
    done
else
    echo ""
    echo "=========================================="
    echo "Experiment completed successfully!"
    echo "Results saved to: result/experiment_results.json"
    echo "=========================================="
fi

