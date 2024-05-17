#! /bin/bash

worker_count=16
tasks_per_worker=8
tasks_length_std=0.01
total_tasks=$((worker_count * tasks_per_worker))

for task_size in 0.1 1 10; do
  for task_duration in 0.01 0.1 1. 10.; do
      python run.py --local-host --use-proxystore --task-count $total_tasks \
        --task-input-size $task_size \
        --task-output-size $task_size \
        --task-length $task_duration \
        --task-length-std $tasks_length_std \
        --worker-count $worker_count
  done
done
