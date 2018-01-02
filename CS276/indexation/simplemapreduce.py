"""
Source : https://pymotw.com/2/multiprocessing/mapreduce.html
Double reduce function
"""
import collections
import itertools
import multiprocessing


class MapDoubleReduce(object):

    def __init__(self, map_func, reduce_func_1, reduce_func_2, num_workers=None):
        """
        map_func

          Function to map inputs to intermediate data. Takes as
          argument one input value and returns a tuple with the key
          and a value to be reduced.

        reduce_func_1 & 2

          Function to reduce partitioned version of intermediate data
          to final output. Takes as argument a key as produced by
          map_func and a sequence of the values associated with that
          key.

        num_workers

          The number of workers to create in the pool. Defaults to the
          number of CPUs available on the current host.
        """
        self.map_func = map_func
        self.reduce_func_1 = reduce_func_1
        self.reduce_func_2 = reduce_func_2
        self.pool = multiprocessing.Pool(num_workers)

    def partition_1(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def partition_2(self, mapped_values):
        """Organize the mapped values by their value.
        Returns an unsorted sequence of tuples with a key and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[value].append(key)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        """Process the inputs through the map and reduce functions given.

        inputs
          An iterable containing the input data to be processed.

        chunksize=1
          The portion of the input data to hand to each worker.  This
          can be used to tune performance during the mapping phase.
        """
        map_responses = self.pool.map(
            self.map_func, inputs, chunksize=chunksize)
        partitioned_data_1 = self.partition_1(itertools.chain(*map_responses))
        reduced_values_1 = self.pool.map(self.reduce_func_1, partitioned_data_1)

        partitioned_data_2 = self.partition_2(itertools.chain(*map_responses))
        reduced_values_2 = self.pool.map(self.reduce_func_2, partitioned_data_2)
        self.pool.close()
        return reduced_values_1, reduced_values_2
