#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark分区策略研究主实验脚本
测试不同分区策略在不同数据分布下的性能表现
观察DAG的Stage划分，记录Shuffle数据量、作业执行时间等关键指标

实验设计：
- 4种数据集：均匀数据、倾斜50%、倾斜70%、倾斜90%
- 3种分区器：HashPartitioner、RangePartitioner、CustomPartitioner
- 总共12个实验
"""

import time
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.rdd import RDD

from generate_data import generate_uniform_data, generate_skewed_data, analyze_data_distribution
from custom_partitioner import apply_salting_strategy


NUM_RECORDS = 2_000_000
NUM_PARTITIONS = 12
NUM_KEYS = 1000
HOT_KEY = "hot_key"
CUSTOM_PARTITIONER_SPLIT_COUNT = 5

DATASET_CONFIGS = [
    {'name': 'skewed_90', 'type': 'skewed', 'skew_ratio': 0.9, 'description': '倾斜90%'},
    {'name': 'skewed_70', 'type': 'skewed', 'skew_ratio': 0.7, 'description': '倾斜70%'},
    {'name': 'skewed_50', 'type': 'skewed', 'skew_ratio': 0.5, 'description': '倾斜50%'},
    {'name': 'uniform', 'type': 'uniform', 'skew_ratio': None, 'description': '均匀分布数据'},
]


def run_experiment(spark: SparkSession, data_rdd: RDD, dataset_name: str, 
                  partitioner_name: str, partitioner_type: str,
                  num_partitions: int = None, use_custom_partitioner: bool = False, 
                  hot_keys: set = None):
    """执行单个实验：对数据应用指定的分区策略，执行reduceByKey操作并记录指标"""
    print(f"\n{'='*60}")
    print(f"Running experiment: {partitioner_name} on {dataset_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        if partitioner_type == 'CustomPartitioner':
            print(f"  Using CustomPartitioner strategy")
            if hot_keys is None:
                hot_keys_param = frozenset()
            elif isinstance(hot_keys, frozenset):
                hot_keys_param = hot_keys
            elif isinstance(hot_keys, (set, list)):
                hot_keys_param = frozenset(hot_keys)
            else:
                hot_keys_param = frozenset([hot_keys])
            
            result_rdd = apply_salting_strategy(
                data_rdd,
                hot_keys=hot_keys_param,
                salt_count=CUSTOM_PARTITIONER_SPLIT_COUNT,
                num_partitions=num_partitions or NUM_PARTITIONS,
                reduce_func=lambda a, b: a + b
            )
        elif partitioner_type == 'RangePartitioner':
            print(f"  Using RangePartitioner (correct implementation with sampling)")
            
            try:
                sc = spark.sparkContext
                num_parts = num_partitions or NUM_PARTITIONS
                jrdd = data_rdd._jrdd
                
                try:
                    range_partitioner = sc._jvm.org.apache.spark.RangePartitioner(
                        num_parts,
                        jrdd,
                        True,
                        sc.defaultParallelism()
                    )
                except:
                    range_partitioner = sc._jvm.org.apache.spark.RangePartitioner(
                        num_parts,
                        jrdd,
                        True
                    )
                
                partitioned_rdd = data_rdd.partitionBy(range_partitioner)
                result_rdd = partitioned_rdd.reduceByKey(lambda a, b: a + b)
                
            except Exception as e:
                print(f"  ⚠️  Warning: Cannot access RangePartitioner via Java gateway: {str(e)}")
                print(f"  Error type: {type(e).__name__}")
                print(f"  Falling back to sortByKey (NOT a true RangePartitioner, much slower)")
                print(f"  This is a fallback only - results will be inaccurate!")
                print(f"  Note: sortByKey performs global sort (O(n log n)), not sampling (O(n))")
                
                sorted_rdd = data_rdd.sortByKey(numPartitions=num_partitions or NUM_PARTITIONS)
                result_rdd = sorted_rdd.reduceByKey(lambda a, b: a + b)
        else:
            print(f"  Using HashPartitioner with {num_partitions or NUM_PARTITIONS} partitions")
            result_rdd = data_rdd.reduceByKey(lambda a, b: a + b, numPartitions=num_partitions or NUM_PARTITIONS)
        
        print(f"  Triggering action (count)...")
        count = result_rdd.count()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"\n  ✓ Experiment completed successfully")
        print(f"  Execution Time: {execution_time:.2f} seconds")
        print(f"  Result Count: {count}")
        
        print(f"\n  ⚠️  IMPORTANT: Please check Spark UI now!")
        print(f"     - DAG Visualization: http://localhost:4040/jobs/")
        print(f"     - Stage Details: http://localhost:4040/stages/")
        print(f"     - Shuffle Metrics: Check 'Shuffle Read Size' and 'Shuffle Write Size'")
        
        if os.environ.get('SPARK_EXPERIMENT_PAUSE', 'false').lower() == 'true':
            input(f"\n  Press Enter to continue to the next experiment...")
        
        return {
            'dataset_name': dataset_name,
            'partitioner_name': partitioner_name,
            'partitioner_type': partitioner_type,
            'execution_time': execution_time,
            'result_count': count,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\n  ✗ Experiment failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'dataset_name': dataset_name,
            'partitioner_name': partitioner_name,
            'partitioner_type': partitioner_type,
            'execution_time': execution_time,
            'result_count': 0,
            'error': str(e),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def save_results(results: list, result_dir: str = "result", filename: str = "experiment_results.json"):
    """保存实验结果到JSON文件"""
    os.makedirs(result_dir, exist_ok=True)
    filepath = os.path.join(result_dir, filename)
    
    output = {
        'experiment_config': {
            'num_records': NUM_RECORDS,
            'num_partitions': NUM_PARTITIONS,
            'num_keys': NUM_KEYS,
            'hot_key': HOT_KEY,
            'custom_partitioner_split_count': CUSTOM_PARTITIONER_SPLIT_COUNT,
            'datasets': [cfg['name'] for cfg in DATASET_CONFIGS],
            'partitioners': ['HashPartitioner', 'RangePartitioner', 'CustomPartitioner']
        },
        'experiments': results,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {filepath}")


def main():
    """主实验函数"""
    print("="*60)
    print("Spark Partition Strategy Experiment")
    print("="*60)
    print(f"\nExperiment Configuration:")
    print(f"  Total Records: {NUM_RECORDS:,}")
    print(f"  Shuffle Partitions: {NUM_PARTITIONS}")
    print(f"  Key Range: 0 to {NUM_KEYS-1}")
    print(f"  Hot Key: {HOT_KEY}")
    print(f"  CustomPartitioner Split Count: {CUSTOM_PARTITIONER_SPLIT_COUNT}")
    print(f"\nDatasets: {len(DATASET_CONFIGS)} types")
    for cfg in DATASET_CONFIGS:
        print(f"  - {cfg['name']}: {cfg['description']}")
    print(f"\nPartitioners: 3 types")
    print(f"  - HashPartitioner")
    print(f"  - RangePartitioner")
    print(f"  - CustomPartitioner (自定义分区策略)")
    print(f"\nTotal Experiments: {len(DATASET_CONFIGS)} × 3 = {len(DATASET_CONFIGS) * 3}")
    
    print(f"\n{'='*60}")
    print("Initializing SparkSession...")
    print(f"{'='*60}")
    
    spark = SparkSession.builder \
        .appName("SparkPartitionerExperiment") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS)) \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()
    
    sc = spark.sparkContext
    print(f"  Spark Version: {sc.version}")
    print(f"  Master URL: {sc.master}")
    print(f"  Default Parallelism: {sc.defaultParallelism}")
    
    all_results = []
    hot_keys = frozenset([HOT_KEY])
    
    for dataset_cfg in DATASET_CONFIGS:
        dataset_name = dataset_cfg['name']
        dataset_type = dataset_cfg['type']
        skew_ratio = dataset_cfg.get('skew_ratio')
        
        print(f"\n{'='*60}")
        print(f"DATASET: {dataset_name.upper()} - {dataset_cfg['description']}")
        print(f"{'='*60}")
        
        if dataset_type == 'uniform':
            print("\nGenerating uniform distribution data...")
            sample_data_rdd = generate_uniform_data(spark, NUM_RECORDS, NUM_KEYS)
        else:
            print(f"\nGenerating skewed distribution data (skew_ratio={skew_ratio})...")
            sample_data_rdd = generate_skewed_data(spark, NUM_RECORDS, NUM_KEYS, HOT_KEY, skew_ratio)
        
        analyze_data_distribution(sample_data_rdd, sample_size=10000)
        
        actual_count = sample_data_rdd.count()
        if actual_count != NUM_RECORDS:
            print(f"  ⚠️  WARNING: Expected {NUM_RECORDS} records, but got {actual_count}")
        
        sample_data_rdd.unpersist()
        
        partitioners = [
            {
                'type': 'HashPartitioner',
                'name': f'HashPartitioner ({dataset_name})',
                'use_custom_partitioner': False
            },
            {
                'type': 'RangePartitioner',
                'name': f'RangePartitioner ({dataset_name})',
                'use_custom_partitioner': False
            },
            {
                'type': 'CustomPartitioner',
                'name': f'CustomPartitioner ({dataset_name})',
                'use_custom_partitioner': True
            }
        ]
        
        for part_cfg in partitioners:
            if dataset_type == 'uniform':
                experiment_data_rdd = generate_uniform_data(spark, NUM_RECORDS, NUM_KEYS)
            else:
                experiment_data_rdd = generate_skewed_data(spark, NUM_RECORDS, NUM_KEYS, HOT_KEY, skew_ratio)
            
            hot_keys_param = hot_keys if part_cfg['use_custom_partitioner'] else None
            result = run_experiment(
                spark=spark,
                data_rdd=experiment_data_rdd,
                dataset_name=dataset_name,
                partitioner_name=part_cfg['name'],
                partitioner_type=part_cfg['type'],
                num_partitions=NUM_PARTITIONS,
                use_custom_partitioner=part_cfg['use_custom_partitioner'],
                hot_keys=hot_keys_param
            )
            all_results.append(result)
            experiment_data_rdd.unpersist()
    
    print(f"\n{'='*60}")
    print("Experiment Summary")
    print(f"{'='*60}")
    
    for dataset_cfg in DATASET_CONFIGS:
        dataset_name = dataset_cfg['name']
        dataset_results = [r for r in all_results if r['dataset_name'] == dataset_name]
        print(f"\n{dataset_cfg['description']} ({dataset_name}):")
        for result in dataset_results:
            status = "✓" if 'error' not in result else "✗"
            print(f"  {status} {result['partitioner_type']}: {result['execution_time']:.2f}s")
            if 'error' in result:
                print(f"    Error: {result['error']}")
    
    result_dir = "result"
    os.makedirs(result_dir, exist_ok=True)
    save_results(all_results, result_dir=result_dir)
    
    spark.stop()
    
    print(f"\n{'='*60}")
    print("All experiments completed!")
    print(f"{'='*60}")
    print("\nNext steps:")
    print("  1. Review Spark UI for detailed metrics")
    print("  2. Run result_analyzer.py to generate analysis and visualizations")
    print("  3. Compare execution times and Shuffle metrics across different strategies")


if __name__ == "__main__":
    main()
