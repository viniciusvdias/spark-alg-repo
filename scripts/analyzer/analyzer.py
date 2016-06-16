import json
import logging
import sys
import numpy
import math

from actions import *
from sparkmodel import *

# thresholds
CORR_THRESHOLD_DEFAULT = 0.7
SKEWNESS_THRESHOLD_DEFAULT = 2

# sources of imbalance
BALANCED = 0
INHERENT = 1
VARIABLE_COST = 2
KEY_DIST = 3

def get_json(line):
   """
   Loads one json line (record). Returns the indexed structure.
   """
   return json.loads(line.strip("\n").replace("\n", "\\n"))

class Analyzer:
   """
   This is the base class for analyzing an spark execution through its logs. The
   user can create add policies and redefine some criteria employed here.
   Specifically the user are encouraged to override the following methods:

   correlation(self, values1, values2): returns a correlation metric between two
   sets of values (0 <= corr <= 1)
      and
   highcorr(self, corr): returns true if 'corr' must be considered a high
   correlation, false otherwise

   skewness(self, values): returns a metric that represents the skewness of a
   set of values.
      and
   highskewness(self,skewness): returns true if 'skewness' must be considered a
   high skewness, false otherwise

   get_opt_funcs(self): return new optimization policies defined by the user,
   i.e, a list of functions with the following signature:
      my_opt_policy(rdd, stages, analyzer)
         :param rdd: represents the adaptive point
         :param stages: the stages that begin at this adaptive point
         :param analyzer: this object for access to functions like correlation
         and skewness
   """

   def __init__(self, filename):
      self.stages = dict()
      self.stages_by_name = dict()
      self.filename = filename

      f = open(filename, "r")
      test_line = f.readline()
      try:
         get_json(test_line)
         is_json = True
      except:
         is_json = False
         f.seek(0)

      for line in f:
         if is_json:
            json_data = get_json(line)
            event_type = json_data["Event"]
            if event_type == "SparkListenerStageSubmitted":
               stage = Stage(json_data)
               self.stages[stage.stage_id] = stage
               same_name = self.stages_by_name.get(stage.stage_name, [])
               self.stages_by_name[stage.stage_name] = same_name + [stage]
            elif event_type == "SparkListenerTaskEnd":
               stage_id = json_data["Stage ID"]
               stage = self.stages[stage_id]
               task = Task(json_data, stage)
               stage.add_task (task)

   def run(self):
      """
      Starts the analyzer, parsing the logfile and applying the defined
      policies.
      Returns a dict with the list of actions (values) for each adaptive point
      found in the code (keys).
      """
      #print ("num_stages=%s" % len(self.stages))
      adaptable = dict()
      for stage_id,stage in self.stages.iteritems():
         if stage.can_adapt():
            first_rdd = stage.first_rdd()
            stages_for_rdd = adaptable.get(first_rdd, [])
            stages_for_rdd.append(stage)
            adaptable[first_rdd] = stages_for_rdd

      sorted_adaptable = sorted(adaptable.iteritems(),
            key=lambda kv: kv[0].rdd_id)

      all_actions = opt_app (sorted_adaptable, self)

      return all_actions
   
   def highskewness(self, skewness):
      return skewness > SKEWNESS_THRESHOLD_DEFAULT

   def highcorr(self, corr):
      return abs(corr) >= CORR_THRESHOLD_DEFAULT
   
   def get_opt_funcs_default(self):
      return [opt_stage_empty_task,
            opt_stage_spill, opt_stage_gc, opt_stage_task_imbalance]

   def get_opt_funcs(self):
      """
      By default this returns an empty list but users are welcome to override
      and include their own policies in that list
      """
      return []

   def opt_funcs(self):
      return self.get_opt_funcs_default() + self.get_opt_funcs()

   def correlation(self, values1, values2):
      """
      Our default correlation considers Pearson correlation
      """
      if not sum(values1) or not sum(values2):
         return 0.0
      else:
         corr = numpy.corrcoef (values1, values2)[0][1]
         if math.isnan(corr):
            return corr
         else:
            return 1.0

   def skewness(self, values):
      """
      Our default measure for skewness considers the ratio between the max value and the
      mean. Therefore, skewness close to 1.0 represent datasets with low skewness
      """
      mean = numpy.mean (values)
      if mean:
         return max(values) / mean
      else:
         return 1.0

def equiv_input(subset, superset):
   """
   Verifies whether one set is subset of another, a.k.a. superset.
   """
   return all(item in superset.iteritems() for item in subset.iteritems())

def is_iterative(adaptable):
   """
   Returns the predicate "is iterative" for each adaptive point.
   """
   iterative = dict()
   for rdd,stages in adaptable:
      iterative[rdd.name] = iterative.get(rdd.name, 0) + 1
   return {r: n > 1 for r,n in iterative.iteritems()}

def is_regular(adaptable):
   """
   Returns the predicate "is regular" for each adaptive point.
   """
   regular = dict()
   for rdd,stages in adaptable:
      records_read = {s.stage_name: s.records_read() for s in stages}
      _records_read = regular.get(rdd.name, {})
      if _records_read:
         if equiv_input(records_read, _records_read):
            regular[rdd.name] = _records_read
         elif equiv_input(_records_read, records_read):
            regular[rdd.name] = records_read
         else:
            regular[rdd.name] = {}
      else:
         regular[rdd.name] = records_read
   return {r: bool(stages) for r,stages in regular.iteritems()}

def opt_stage_empty_task(rdd, stages, analyzer):
   # select super-stage, i.e., that has the most RDDs
   superstage = max(stages, key = lambda s: len(s.rdds))
   num_empty_tasks = len(superstage.empty_tasks())
   if num_empty_tasks:
      return UpdateNumPartitions(rdd.name,
            superstage.num_tasks - num_empty_tasks)

def opt_stage_spill(rdd, stages, analyzer):
   superstage = max(stages, key = lambda s: len(s.rdds))
   shuffle_write_bytes = superstage.shuffle_write_bytes()
   if shuffle_write_bytes > 0:
      factor = superstage.bytes_spilled() / shuffle_write_bytes
      if factor:
         new_num_partitions = factor * superstage.num_tasks
         return UpdateNumPartitions(rdd.name, new_num_partitions)

def opt_stage_gc(rdd, stages, analyzer):
   superstage = max(stages, key = lambda s: len(s.rdds))
   gc_times = superstage.task_gc_times()
   _skewness = analyzer.skewness(gc_times)
   if (analyzer.highskewness(_skewness)):
      min_gc = min (gc_times)
      if min_gc:
         new_num_partitions = (max(gc_times) / min_gc) * superstage.num_tasks
         return UpdateNumPartitions(rdd.name, new_num_partitions)

def source_of_imbalance(stage, analyzer):
   """
   Returns the source of imbalance (or the lack of it) for task run times.
   """
   run_times = stage.task_run_times()
   _skewness = analyzer.skewness (run_times)
   if not analyzer.highskewness(_skewness):
      return BALANCED

   corr1 = analyzer.correlation (run_times, stage.task_shuffle_read_bytes())
   corr2 = analyzer.correlation (run_times, stage.task_shuffle_read_records())
   highcorr1 = analyzer.highcorr(corr1)
   highcorr2 = analyzer.highcorr(corr2)

   if highcorr1 and highcorr2:
      return KEY_DIST
   elif not highcorr1 and not highcorr2:
      return INHERENT
   else:
      return VARIABLE_COST

def opt_stage_task_imbalance(rdd, stages, analyzer):
   superstage = max(stages, key = lambda s: len(s.rdds))
   imbalance = source_of_imbalance(superstage, analyzer)
   if imbalance == INHERENT:
      return Warn(rdd.name, "the imbalance is inherent, please check the UDF that" +
            " generates this RDD")
   elif imbalance == KEY_DIST:
      return UpdatePartitioner(rdd.name, "rangePartitioner")
   elif imbalance == VARIABLE_COST:
      return Warn(rdd.name, "consider to specialize the partitioner considering the" +
            " layout of your data")
   elif imbalance == BALANCED:
      return NoAction(rdd.name)
   else:
      raise ValueError('Should never happen: unrecognized source of imbalance')

def opt_stage(rdd, stages, analyzer):
   for func in analyzer.opt_funcs():
      #print ("applying optimization %s" % func.__name__)
      action = func(rdd, stages, analyzer)
      if action: return action

def opt_noniterative(adaptable, analyzer):
   actions = {}
   for rdd,stages in adaptable:
      action = opt_stage (rdd, stages, analyzer)
      actions[rdd.name] = actions.get(rdd.name, []) + [action]
   return actions

def opt_iterative_regular(adaptable, analyzer):
   actions = {}
   for rdd,stages in adaptable:
      if rdd.name in actions:
         # copy plan
         actions[rdd.name] = actions[rdd.name] + [actions[rdd.name][0]]
      else:
         # first iteration
         _actions = opt_noniterative([(rdd,stages)], analyzer)
         actions[rdd.name] = _actions.values()[0]

   return actions

def opt_iterative_irregular(adaptable, analyzer):
   actions = {}
   first_input = {}
   for rdd,stages in adaptable:
      if rdd.name in actions and rdd.name in first_input:
         # scale and copy plan
         superstage = max(stages, key = lambda s: len(s.rdds))
         factor = float(superstage.records_read()) / first_input[rdd.name]
         first_action = actions[rdd.name][0]
         action = first_action.scaled(factor)
         actions[rdd.name] = actions[rdd.name] + [action]
      else:
         # first iteration
         superstage = max(stages, key = lambda s: len(s.rdds))
         _actions = opt_noniterative([(rdd,stages)], analyzer)
         actions[rdd.name] = _actions.values()[0]
         first_input[rdd.name] = superstage.records_read()

   return actions

def opt_app(adaptable, analyzer):
   # find adaptive points categories
   iteratives = is_iterative (adaptable)
   regulars = is_regular (adaptable)

   # separate by categories
   adaptive_points = {}
   for rdd,stages in adaptable:
      ap = adaptive_points.get (rdd.name, {})
      ap[rdd] = stages
      adaptive_points[rdd.name] = ap

   all_actions = {}
   for adpt_point,instances in adaptive_points.iteritems():
      iterative = iteratives[adpt_point]
      regular = regulars[adpt_point]
      if iterative and regular:
         sorted_instances = sorted(instances.iteritems(), key=lambda kv: kv[0].rdd_id)
         actions = opt_iterative_regular (sorted_instances, analyzer)
         all_actions.update (actions)
      elif iterative and not regular:
         sorted_instances = sorted(instances.iteritems(), key=lambda kv: kv[0].rdd_id)
         actions = opt_iterative_irregular (sorted_instances, analyzer)
         all_actions.update (actions)
      elif not iterative:
         sorted_instances = sorted(instances.iteritems(), key=lambda kv: kv[0].rdd_id)
         actions = opt_noniterative (sorted_instances, analyzer)
         all_actions.update (actions)
      else:
         raise ValueError ("Could not determine the AP(%s) category" %
               adpt_point)

   return all_actions

# vim: tabstop=8 expandtab shiftwidth=3 softtabstop=3
