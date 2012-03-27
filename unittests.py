#!/usr/bin/python

import unittest
import os, glob
from persistent_queue_with_acknowledgement import PersistentQueueWithAcknowledgement
from persistent_queue_with_acknowledgement import Persistent

class Something:
	def __init__(self, obj):
		self.obj = obj

	def __repr__(self):
		return repr(self.obj)

	def __str__(self):
		return str(self.obj)

	def __unicode__(self):
		return unicode(self.obj)

class BasicWrapper(Persistent):
#class BasicWrapper(persistent.Persistent):
#class BasicWrapper:
	def __init__(self, obj):
		self.obj = obj

	def __repr__(self):
		return repr(self.obj)

	def __str__(self):
		return str(self.obj)

	def __unicode__(self):
		return unicode(self.obj)

BW = BasicWrapper

class TestPersistentQueueWithAcknowledgement(unittest.TestCase):
	def setUp(self):
		for filename in glob.glob('test_queue.fs*') :
			os.remove( filename ) 

	def tearDown(self):
		for filename in glob.glob('test_queue.fs*') :
			os.remove( filename ) 


	def test_basic_single_thread_1(self):
		q = PersistentQueueWithAcknowledgement("test_queue.fs")
		for i in xrange(10):
			q.put(BW(Something(i)))

		myset = set()

		while not q.empty():
			i = q.get()
			myset.add(i)

		# Check that all elements are on the nonack queue and no elements is on the main queue
		self.assertTrue(len(q._queue.elements.items()) == 0)

		# Check that all elements are on the nonack set
		nonack_set = set()
		for item in q._nonack_queue.keys():
			nonack_set.add(item)

		self.assertEqual(myset, nonack_set)

	def test_basic_single_thread_2(self):
		q = PersistentQueueWithAcknowledgement("test_queue.fs")
		for i in xrange(10):
			q.put(BW(Something(i)))

		myset = set()
		items = []

		while not q.empty():
			i = q.get()
			myset.add(i)
			items.append(i)

		# ack last 5 items
		for item in items[4:]:
			q.ack(item)
			myset.remove(item)


		# Check that all elements are on the nonack queue and no elements is on the main queue
		self.assertTrue(len(q._queue.elements.items()) == 0)

		# Check that five elements are on the nonack set
		nonack_set = set()
		for item in q._nonack_queue.keys():
			nonack_set.add(item)

		self.assertEqual(myset, nonack_set)

	def dummy_destructor(self):
		pass

	def test_power_outage_single_thread(self):
		q = PersistentQueueWithAcknowledgement("test_queue.fs")
		for i in xrange(10):
			q.put(BW(Something(i)))

		myset = set()

		while not q.empty():
			i = q.get()
			myset.add(i)

		# Check that all elements are on the nonack queue and no elements is on the main queue
		self.assertTrue(len(q._queue.elements.items()) == 0)

		# Check that all elements are on the nonack set
		nonack_set = set()
		for item in q._nonack_queue.keys():
			nonack_set.add(item)

		self.assertEqual(myset, nonack_set)

		q._destructor = self.dummy_destructor

		q.connection.close()
		q.db.close()
		q.storage.close()
#		q._dump()
		del q

		# Check that all elements are on the queue again and nothing in the nonack set
		q = PersistentQueueWithAcknowledgement("test_queue.fs")
		self.assertTrue(len(q._queue.elements.items()) == 10)
		self.assertTrue(len(q._nonack_queue.keys()) == 0)
#		q._dump()
		




def test():
	q = PersistentQueueWithAcknowledgement("test_basic_single_thread.fs")
	for i in xrange(10):
		q.put(BW(Something(i)))

	while not q.empty():
		i = q.get()

	q._dump()
	del q

def test2():
	q = PersistentQueueWithAcknowledgement("test_basic_single_thread.fs")
	q._dump()

	while not q.empty():
		i = q.get()







if __name__ == "__main__":
	unittest.main()
#	test()
#	test2()
