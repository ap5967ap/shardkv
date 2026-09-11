#!/usr/bin/env python3
"""
Analyze benchmark results and generate latency charts.
"""

import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict
try:
    import numpy as np
    import matplotlib.pyplot as plt
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False
    print("Warning: numpy/matplotlib not available. Plotting disabled.")


def load_results(filename):
    """Load results from JSONL file."""
    results = []
    with open(filename, 'r') as f:
        for line in f:
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return results


def calculate_percentiles(latencies_ms):
    """Calculate p50, p95, p99 latencies."""
    if not latencies_ms:
        return 0, 0, 0
    if HAS_PLOTTING:
        return (
            np.percentile(latencies_ms, 50),
            np.percentile(latencies_ms, 95),
            np.percentile(latencies_ms, 99)
        )
    else:
        # Fallback without numpy
        sorted_latencies = sorted(latencies_ms)
        n = len(sorted_latencies)
        return (
            sorted_latencies[n // 2],
            sorted_latencies[int(n * 0.95)],
            sorted_latencies[int(n * 0.99)]
        )


def analyze_by_operation(results):
    """Analyze results by operation type."""
    by_op = defaultdict(list)
    for result in results:
        op = result['operation']
        latency_ms = result['latency_ms']  # Already in milliseconds
        by_op[op].append(latency_ms)
    
    stats = {}
    for op, latencies in by_op.items():
        p50, p95, p99 = calculate_percentiles(latencies)
        mean_ms = sum(latencies) / len(latencies) if latencies else 0
        stats[op] = {
            'count': len(latencies),
            'p50_ms': p50,
            'p95_ms': p95,
            'p99_ms': p99,
            'mean_ms': mean_ms,
            'success_rate': sum(1 for r in results if r['operation'] == op and r['success']) / len(latencies)
        }
    
    return stats


def analyze_by_consistency(results):
    """Analyze GET results by consistency level."""
    # Group GET latencies by their consistency label (strong/eventual)
    get_results = [r for r in results if r.get('operation') == 'GET']
    by_consistency = defaultdict(list)

    for result in get_results:
        consistency = result.get('consistency', 'unknown')
        latency_ms = result.get('latency_ms', 0.0)
        by_consistency[consistency].append(latency_ms)

    stats = {}
    for consistency, latencies in by_consistency.items():
        p50, p95, p99 = calculate_percentiles(latencies)
        if HAS_PLOTTING:
            mean_ms = float(np.mean(latencies)) if latencies else 0.0
        else:
            mean_ms = sum(latencies) / len(latencies) if latencies else 0.0
        stats[consistency] = {
            'count': len(latencies),
            'p50_ms': p50,
            'p95_ms': p95,
            'p99_ms': p99,
            'mean_ms': mean_ms
        }

    return stats


def plot_latency_comparison(results_files, output_dir):
    """Plot latency comparison across multiple result files."""
    if not HAS_PLOTTING:
        print("Skipping latency comparison plot - matplotlib not available")
        return
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Latency Comparison Across Configurations', fontsize=16)
    
    all_stats = {}
    for filename in results_files:
        results = load_results(filename)
        name = Path(filename).stem
        all_stats[name] = analyze_by_operation(results)
    
    # Plot 1: P50 latency by operation
    ax = axes[0, 0]
    operations = ['PUT', 'GET']
    x = np.arange(len(operations))
    width = 0.8 / len(all_stats)
    
    for i, (name, stats) in enumerate(all_stats.items()):
        p50_values = [stats.get(op, {}).get('p50_ms', 0) for op in operations]
        ax.bar(x + i * width, p50_values, width, label=name)
    
    ax.set_xlabel('Operation')
    ax.set_ylabel('P50 Latency (ms)')
    ax.set_title('P50 Latency by Operation')
    ax.set_xticks(x + width * (len(all_stats) - 1) / 2)
    ax.set_xticklabels(operations)
    ax.legend()
    
    # Plot 2: P95 latency by operation
    ax = axes[0, 1]
    for i, (name, stats) in enumerate(all_stats.items()):
        p95_values = [stats.get(op, {}).get('p95_ms', 0) for op in operations]
        ax.bar(x + i * width, p95_values, width, label=name)
    
    ax.set_xlabel('Operation')
    ax.set_ylabel('P95 Latency (ms)')
    ax.set_title('P95 Latency by Operation')
    ax.set_xticks(x + width * (len(all_stats) - 1) / 2)
    ax.set_xticklabels(operations)
    ax.legend()
    
    # Plot 3: P99 latency by operation
    ax = axes[1, 0]
    for i, (name, stats) in enumerate(all_stats.items()):
        p99_values = [stats.get(op, {}).get('p99_ms', 0) for op in operations]
        ax.bar(x + i * width, p99_values, width, label=name)
    
    ax.set_xlabel('Operation')
    ax.set_ylabel('P99 Latency (ms)')
    ax.set_title('P99 Latency by Operation')
    ax.set_xticks(x + width * (len(all_stats) - 1) / 2)
    ax.set_xticklabels(operations)
    ax.legend()
    
    # Plot 4: Success rate
    ax = axes[1, 1]
    for i, (name, stats) in enumerate(all_stats.items()):
        success_rates = [stats.get(op, {}).get('success_rate', 0) * 100 for op in operations]
        ax.bar(x + i * width, success_rates, width, label=name)
    
    ax.set_xlabel('Operation')
    ax.set_ylabel('Success Rate (%)')
    ax.set_title('Success Rate by Operation')
    ax.set_xticks(x + width * (len(all_stats) - 1) / 2)
    ax.set_xticklabels(operations)
    ax.legend()
    ax.set_ylim([0, 105])
    
    plt.tight_layout()
    output_path = Path(output_dir) / 'latency_comparison.png'
    plt.savefig(output_path)
    print(f"Saved latency comparison plot to {output_path}")
    plt.close()


def plot_latency_distribution(results_file, output_dir):
    """Plot latency distribution for a single result file."""
    if not HAS_PLOTTING:
        print("Skipping latency distribution plot - matplotlib not available")
        return
    results = load_results(results_file)
    name = Path(results_file).stem
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f'Latency Distribution - {name}', fontsize=16)
    
    # PUT latency distribution
    put_latencies = [r['latency_ms'] for r in results if r['operation'] == 'PUT']
    if put_latencies:
        axes[0].hist(put_latencies, bins=50, alpha=0.7, color='blue')
        axes[0].set_xlabel('Latency (ms)')
        axes[0].set_ylabel('Frequency')
        axes[0].set_title('PUT Latency Distribution')
        if HAS_PLOTTING:
            median_put = np.median(put_latencies)
        else:
            sorted_put = sorted(put_latencies)
            median_put = sorted_put[len(sorted_put)//2]
        axes[0].axvline(median_put, color='red', linestyle='--', label=f'Median: {median_put:.2f}ms')
        axes[0].legend()
    
    # GET latency distribution
    get_latencies = [r['latency_ms'] for r in results if r['operation'] == 'GET']
    if get_latencies:
        axes[1].hist(get_latencies, bins=50, alpha=0.7, color='green')
        axes[1].set_xlabel('Latency (ms)')
        axes[1].set_ylabel('Frequency')
        axes[1].set_title('GET Latency Distribution')
        if HAS_PLOTTING:
            median_get = np.median(get_latencies)
        else:
            sorted_get = sorted(get_latencies)
            median_get = sorted_get[len(sorted_get)//2]
        axes[1].axvline(median_get, color='red', linestyle='--', label=f'Median: {median_get:.2f}ms')
        axes[1].legend()
    
    plt.tight_layout()
    output_path = Path(output_dir) / f'{name}_distribution.png'
    plt.savefig(output_path)
    print(f"Saved latency distribution plot to {output_path}")
    plt.close()


def print_summary(results_file):
    """Print summary statistics for a result file."""
    results = load_results(results_file)
    stats = analyze_by_operation(results)
    
    print(f"\n=== Summary for {results_file} ===")
    print(f"Total requests: {len(results)}")
    
    for op, op_stats in stats.items():
        print(f"\n{op}:")
        print(f"  Count: {op_stats['count']}")
        print(f"  P50: {op_stats['p50_ms']:.2f}ms")
        print(f"  P95: {op_stats['p95_ms']:.2f}ms")
        print(f"  P99: {op_stats['p99_ms']:.2f}ms")
        print(f"  Mean: {op_stats['mean_ms']:.2f}ms")
        print(f"  Success rate: {op_stats['success_rate']*100:.1f}%")

    # Also print GET stats grouped by consistency (strong/eventual)
    consistency_stats = analyze_by_consistency(results)
    if consistency_stats:
        print(f"\nGET by consistency:")
        for consistency, cstats in consistency_stats.items():
            print(f"  {consistency}:")
            print(f"    Count: {cstats['count']}")
            print(f"    P50: {cstats['p50_ms']:.2f}ms")
            print(f"    P95: {cstats['p95_ms']:.2f}ms")
            print(f"    P99: {cstats['p99_ms']:.2f}ms")
            print(f"    Mean: {cstats['mean_ms']:.2f}ms")


def main():
    parser = argparse.ArgumentParser(description='Analyze benchmark results')
    parser.add_argument('results_files', nargs='+', help='Result files to analyze')
    parser.add_argument('--output-dir', default='.', help='Output directory for plots')
    parser.add_argument('--summary-only', action='store_true', help='Only print summary, no plots')
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Print summary for each file
    for results_file in args.results_files:
        print_summary(results_file)
    
    if not args.summary_only:
        # Generate comparison plot if multiple files
        if len(args.results_files) > 1:
            plot_latency_comparison(args.results_files, output_dir)
        
        # Generate distribution plots for each file
        for results_file in args.results_files:
            plot_latency_distribution(results_file, output_dir)


if __name__ == '__main__':
    main()