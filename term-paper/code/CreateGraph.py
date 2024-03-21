import json
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

# Load time used from JSON file
with open('term-paper/data/time_data.json', 'r') as file:
    data = json.load(file)
    nosql_time_used = data['nosql']
    hybrid_time_used = data['hybrid']
    rds_time_used = data['rds']
    sensitive_time_used = data['sensitive']


execution_count = list(range(1, 1001))
total_time_nosql = []
total_time_hybrid = []
total_time_rds = []
total_time_sensitive = []

for i in range(1, 1001):
    total_time_nosql.append(sum(nosql_time_used[:i]))
    total_time_hybrid.append(sum(hybrid_time_used[:i]))
    total_time_rds.append(sum(rds_time_used[:i]))
    total_time_sensitive.append(sum(sensitive_time_used[:i]))

plt.plot(execution_count, total_time_nosql, marker='o', label = 'Hybrid Datastore (best case)')
plt.plot(execution_count, total_time_hybrid, marker='o', label = 'Hybrid Datastore (worst case)')
plt.plot(execution_count, total_time_rds, marker='o', label = 'Relational Database (best case)')
plt.plot(execution_count, total_time_sensitive, marker='o', label = 'Hybrid Datastore (sensitive field)')
plt.xlabel('Number of Executions')
plt.ylabel('Total Time (seconds)')
plt.title('Execution Time Comparison')
plt.legend()
plt.savefig('term-paper/data/general.png')


total_time_50_50 = [(nosql + hybrid) / 2 for nosql, hybrid in zip(total_time_nosql, total_time_hybrid)]
total_time_25_75 = [(nosql * 0.75 + hybrid * 0.25) for nosql, hybrid in zip(total_time_nosql, total_time_hybrid)]
total_time_10_90 = [(nosql * 0.90 + hybrid * 0.10) for nosql, hybrid in zip(total_time_nosql, total_time_hybrid)]

plt.figure(figsize=(10, 6))
plt.plot(execution_count, total_time_rds, marker='o', label='Relational Database (best case)')
plt.plot(execution_count, total_time_50_50, marker='o', label='50% Miss Rate (Hybrid Datastore)')
plt.plot(execution_count, total_time_25_75, marker='o', label='25% Miss Rate (Hybrid Datastore)')
plt.plot(execution_count, total_time_10_90, marker='o', label='10% Miss Rate (Hybrid Datastore)')
plt.plot(execution_count, total_time_nosql, marker='o', label = 'Hybrid Datastore (best case)')
plt.xlabel('Number of Executions')
plt.ylabel('Total Time (seconds)')
plt.title('Execution Time Comparison')
plt.legend()
plt.savefig('term-paper/data/comparison.png')
plt.show()

