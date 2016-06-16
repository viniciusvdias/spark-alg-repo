import math

class Action:
   """
   Every action has its own adaptive point.
   """

   def __init__(self, adaptive_point):
      self.adaptive_point = adaptive_point

class NoAction(Action):
   """
   No other action is suitable for this adaptive point.
   """

   def __init__(self, adaptive_point):
      """
      Construct a new 'NoAction' object

      :param adaptive_point: the name representing an adaptive point in the code
      """
      Action.__init__(self, adaptive_point)

   def __repr__(self):
      return "no-act,%s" % self.adaptive_point

class UpdateNumPartitions(Action):
   """
   With this action we keep the same partitioner and change the number of
   partitions of this adaptive point.
   """

   def __init__(self, adaptive_point, num_partitions):
      """
      Construct a new 'UpdateNumPartitions' object.

      :param adaptive_point: the name representing an adaptive point in the code
      :param num_partitions: the new number of partitions
      """
      Action.__init__(self, adaptive_point)
      self.num_partitions = num_partitions

   def scaled(self, factor):
      """
      This function is called when one need to scale this action w.r.t. a factor

      :param factor: a float number that can increase (>1.0) or decrease (>=0
      and <1) the number of partitions of a new action.
      :return: new modified action
      """
      new_num_partitions = max(1, int(math.ceil (factor * self.num_partitions)))
      new_action = UpdateNumPartitions(self.adaptive_point, new_num_partitions)
      return new_action

   def __repr__(self):
      return "act-unp,%s,%s" % (self.adaptive_point,
            self.num_partitions)

class UpdatePartitioner(Action):
   """
   With this action we keep the same number of partitions and change the
   partitioning strategy, i.e., the partitioner. Supported values:
   - rangePartitioner
   - hashPartitioner
   """

   KNOWN_PARTITIONERS = ["hashPartitioner", "rangePartitioner"]

   def __init__(self, adaptive_point, partitioner):
      """
      Construct a new 'UpdatePartitioner' object.

      :param adaptive_point: the name representing an adaptive point in the code
      :param partitioner: a string representing which partitioner should be
      employed
      """
      Action.__init__(self, adaptive_point)
      self.num_partitions = num_partitions

      new_num_partitions = max(1, int(math.ceil (factor * self.num_partitions)))
      new_action = UpdateNumPartitions(self.adaptive_point, new_num_partitions)
      return new_action
      Action.__init__(self, adaptive_point)
      assert adaptive_point in KNOWN_PARTITIONERS
      self.partitioner = partitioner

   def __repr__(self):
      return "act-up,%s,%s" % (self.adaptive_point,
            self.partitioner)

class Warn(Action):
   """
   No automatic action is possible. Usually this is related to specifics of the
   user code. Thus we just warn the user.
   """

   def __init__(self, adaptive_point, msg):
      """
      Construct a new 'Warn' object.

      :param adaptive_point: the name representing an adaptive point in the code
      :param msg: the message indicating the possible issue in the code
      """
      Action.__init__(self, adaptive_point)
      self.num_partitions = num_partitions

      Action.__init__(self, adaptive_point)
      self.msg = msg

   def __repr__(self):
      return "act-wrn,%s,%s" % (self.adaptive_point, self.msg)

# vim: tabstop=8 expandtab shiftwidth=3 softtabstop=3
