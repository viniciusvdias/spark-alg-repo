import json
import logging
import sys
import numpy
import math

# consider only rdds flagged as adaptive
ONLY_ADAPTIVE = False

def get_or_else(data, key, default_value = None):
   """
   Tries to get a value from data with key. Returns default_value in case of key
   not found.
   """
   if not data:
      return default_value
   try:
      return data[key]
   except:
      return default_value

class Stage:
   def __init__(self, stage_data):
      self.stage_id = stage_data["Stage Info"]["Stage ID"]
      self.stage_name = stage_data["Stage Info"]["Stage Name"]
      self.num_tasks = int(stage_data["Stage Info"]["Number of Tasks"])
      self.tasks = self.num_tasks * [None]
      self.parent_ids = []
      self.rdds = []
      for parent_id in stage_data["Stage Info"]["Parent IDs"]:
         self.parent_ids.append (int(parent_id))
      for rdd_info in stage_data["Stage Info"]["RDD Info"]:
         self.rdds.append (RDD(rdd_info))

   def add_task(self, task):
      self.tasks[task.task_idx] = task

   def first_rdd(self):
      return min([r for r in self.rdds], key = lambda r: r.rdd_id)

   def empty_tasks(self):
      return [t for t in self.tasks if t.records_read() == 0]

   def adaptive_point(self):
      first_rdd = self.first_rdd()
      if "adaptive" in first_rdd.name:
         return first_rdd.name
      else:
         return "-"

   def can_adapt(self):
      return not ONLY_ADAPTIVE or "adaptive" in self.adaptive_point()

   def task_run_times(self):
      return [t.run_time for t in self.tasks if t]

   def task_gc_times(self):
      return [t.gc_time for t in self.tasks if t]

   def task_shuffle_read_bytes(self):
      return [t.shuffle_read_bytes for t in self.tasks if t]

   def task_shuffle_read_records(self):
      return [t.shuffle_read_records for t in self.tasks if t]

   def input_records(self):
      return sum([t.input_records for t in self.tasks if t])
   
   def shuffle_read_records(self):
      return sum([t.shuffle_read_records for t in self.tasks if t])

   def shuffle_read_bytes(self):
      return sum([t.shuffle_read_bytes for t in self.tasks if t])

   def shuffle_write_records(self):
      return sum([t.shuffle_write_records for t in self.tasks if t])

   def shuffle_write_bytes(self):
      return sum([t.shuffle_write_bytes for t in self.tasks if t])

   def bytes_spilled(self):
      return sum([t.bytes_spilled() for t in self.tasks if t])

   def records_read(self):
      return self.input_records() + self.shuffle_read_records()

   def __repr__(self):
      return "Stage(stage_id=%s,name=%s,num_tasks=%s,parents=%s)" % (
            self.stage_id, self.stage_name, self.num_tasks, self.parent_ids)

class RDD:
   def __init__(self, rdd_info):
      self.rdd_id = rdd_info["RDD ID"]
      self.name = rdd_info["Name"]
      self.parent_ids = rdd_info["Parent IDs"]
      self.callsite = get_or_else (rdd_info, "Callsite", "callsite-unavailable")

   def first_rdd(self):
      if not self.parent_ids:
         return self
      else:
         assert self.parent_ids
         return

   def __eq__(r1, r2):
      return r1.rdd_id == r2.rdd_id

   def __hash__(self):
      return hash((self.rdd_id, self.name))

   def __repr__(self):
      return "RDD(%s,%s,%s)" % (self.rdd_id, self.name, self.callsite)

class Task:
   def __init__(self, task_data, stage):
      self.task_idx = int(task_data["Task Info"]["Index"])

      # run times
      self.run_time = get_or_else (task_data["Task Metrics"],
         "Executor Run Time", 0)

      # garbage collection
      self.gc_time = get_or_else (task_data["Task Metrics"], "JVM GC Time", 0)
      
      # input metrics
      input_metrics = get_or_else (task_data["Task Metrics"], "Input Metrics")
      self.input_records = int(get_or_else(input_metrics, "Records Read", 0))

      # spill
      self.memory_bytes_spilled = get_or_else (task_data["Task Metrics"],
            "Memory Bytes Spilled")
      self.disk_bytes_spilled = get_or_else (task_data["Task Metrics"],
            "Disk Bytes Spilled")

      # shuffle read metrics
      shuffle_read_metrics = get_or_else (task_data["Task Metrics"],
         "Shuffle Read Metrics")
      self.shuffle_read_records = int(get_or_else (shuffle_read_metrics,
         "Total Records Read", 0))
      self.shuffle_read_bytes = int(get_or_else (shuffle_read_metrics,
         "Local Bytes Read", 0)) + int(get_or_else (shuffle_read_metrics,
         "Remote Bytes Read", 0))
      self.stage = stage
      
      # shuffle write metrics
      shuffle_write_metrics = get_or_else (task_data["Task Metrics"],
         "Shuffle Write Metrics")
      self.shuffle_write_records = int(get_or_else (shuffle_write_metrics,
         "Shuffle Records Written", 0))
      self.shuffle_write_bytes = int(get_or_else (shuffle_write_metrics,
         "Shuffle Bytes Written", 0))
      self.stage = stage

   def records_read(self):
      return self.input_records + self.shuffle_read_records

   def bytes_spilled(self):
      return self.memory_bytes_spilled + self.disk_bytes_spilled

   def __repr__(self):
      return "Task(%s,%s)" % (self.stage.stage_id, self.task_idx) 

# vim: tabstop=8 expandtab shiftwidth=3 softtabstop=3
