#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据生成模块
生成均匀分布和倾斜分布的测试数据，用于Spark分区策略研究实验
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext


def generate_uniform_data(spark: SparkSession, num_records: int, num_keys: int = 1000):
    """生成均匀分布的测试数据"""
    sc = spark.sparkContext
    data = range(num_records)
    rdd = sc.parallelize(data).map(lambda i: (f"key_{i % num_keys}", i))
    
    print(f"Generated {num_records} records with UNIFORM distribution (for experiment).")
    print(f"  Key range: key_0 to key_{num_keys-1}")
    print(f"  Records per key (approx): {num_records // num_keys}")
    print(f"  Initial partitions: {rdd.getNumPartitions()}")
    print(f"  Note: Data is NOT repartitioned - experiment partitioners will handle distribution")
    return rdd


def generate_skewed_data(spark: SparkSession, num_records: int, num_keys: int = 1000, 
                        skew_key: str = "hot_key", skew_ratio: float = 0.9):
    """生成倾斜分布的测试数据"""
    sc = spark.sparkContext
    
    skew_count = int(num_records * skew_ratio)
    normal_count = num_records - skew_count
    
    skew_data = [(skew_key, i) for i in range(skew_count)]
    normal_data = [(f"key_{i % num_keys}", i) for i in range(normal_count)]
    all_data = skew_data + normal_data
    
    rdd = sc.parallelize(all_data)
    
    print(f"Generated {num_records} records with SKEW distribution (for experiment).")
    print(f"  Hot key: {skew_key} ({skew_count} records, {skew_ratio*100:.1f}%)")
    print(f"  Other keys: key_0 to key_{num_keys-1} ({normal_count} records, {(1-skew_ratio)*100:.1f}%)")
    print(f"  Initial partitions: {rdd.getNumPartitions()}")
    print(f"  Note: Data is NOT shuffled or repartitioned - experiment partitioners will handle distribution")
    print(f"  Warning: This data is intentionally skewed - HashPartitioner will suffer from data skew!")
    
    return rdd


def analyze_data_distribution(rdd, sample_size: int = 10000):
    """分析数据分布情况"""
    sample = rdd.sample(False, min(1.0, sample_size / rdd.count()))
    key_counts = sample.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b).collect()
    key_counts_dict = dict(key_counts)
    
    counts = list(key_counts_dict.values())
    if counts:
        max_count = max(counts)
        min_count = min(counts)
        avg_count = sum(counts) / len(counts)
        
        hot_keys = [k for k, v in key_counts_dict.items() if v > avg_count * 2]
        
        print(f"\nData Distribution Analysis (sample size: {len(key_counts)} keys):")
        print(f"  Total unique keys in sample: {len(key_counts)}")
        print(f"  Max records per key: {max_count}")
        print(f"  Min records per key: {min_count}")
        print(f"  Avg records per key: {avg_count:.2f}")
        if hot_keys:
            print(f"  Hot keys (count > 2*avg): {hot_keys[:5]}...")
        
        return {
            'total_keys': len(key_counts),
            'max_count': max_count,
            'min_count': min_count,
            'avg_count': avg_count,
            'hot_keys': hot_keys
        }
    return {}

