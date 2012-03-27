#!/usr/bin/python

from ZODB.FileStorage import FileStorage
from ZODB.DB import DB
from BTrees.IOBTree import IOBTree
from BTrees.OOBTree import OOTreeSet
import transaction
import persistent
from threading import Thread, Lock
import copy
import time
import sys
import weakref
from Queue import Queue

MAX_INT = sys.maxint
MIN_INT = -MAX_INT - 1

#DEBUG = True
DEBUG = False


class SizeOfPersistentQueueExceeded(Exception):
	pass

class ObjectIsDuplicatedOnNonAcknowledgedSet(Exception):
	pass

class PersistentDeque(persistent.Persistent):
	def __init__(self):
		self.start = 0
		self.stop = 0
		self.elements = IOBTree()

Persistent = persistent.Persistent

class PersistentQueueWithAcknowledgement(Queue):
	def __init__(self, filename):
		self.is_first = True
		self.connect_mutex = Lock()
		self.shared_data = {}
		self.filename = filename
		self.ack_index = 0
		Queue.__init__(self) # initialized with maxsize = 0. Assuming that for all the operations
		self.connect(self.is_first)

	def _init(self, maxsize):
		self._init_db()
		pass

	def _init_db(self):
		self.storage = FileStorage(self.filename)
		self.db = DB(self.storage)

	def _qsize(self, len = len):
		self.connection.transaction_manager.begin()
		if self._queue.stop >= self._queue.start:
			size = self._queue.stop - self._queue.start
		else:
			size (MAX_INT - self._queue.start + 1) + (self._queue.stop - MIN_INT)
		self.connection.transaction_manager.commit()

		return size

	def _put_in_queue(self, queue, item):
		queue.elements[queue.stop] = item
		queue.stop += 1
		if queue.stop > MAX_INT:
			# check also if stop touches start
			queue.stop = MIN_INT

		if queue.start == queue.stop:
			raise SizeOfPersistentQueueExceeded

	def _put_in_queue_head(self, queue, item):
		queue.start -= 1
		if queue.start < MIN_INT:
			queue.start = MAX_INT

		if queue.start == queue.stop:
			raise SizeOfPersistentQueueExceeded

		queue.elements[queue.start] = item

	def _put(self, item):
		self.connection.transaction_manager.begin()
		self._put_in_queue(self._queue, item)
		self.connection.transaction_manager.commit()


	def _get_from_queue(self, queue):
		item = queue.elements[queue.start]
		del queue.elements[queue.start]
		queue.start += 1
		if queue.start > MAX_INT:
			# check also if start touches stop
			queue.start = MIN_INT 

		if queue.start == queue.stop: # if queue is empty resync start & stop to 0. It is for beautifier purposes can be removed.
			queue.start = 0
			queue.stop = 0

		return item

	def _get(self):
		self.connection.transaction_manager.begin()
		item = self._get_from_queue(self._queue) # get from main queue
		if item in self._nonack_queue:
			raise ObjectIsDuplicatedOnNonAcknowledgedSet

		self._nonack_queue.add(item)
		self.connection.transaction_manager.commit()

		return item

	def _dump(self): # for debugging purposes only. Dumps queue and non acknowledge queues
		self.connection.transaction_manager.begin()
		print "--------- Dump ----------"
		print "# of items in root['queue'] =", len(self._queue.elements.items())
		print "Dumping root['queue']"
		for k,v in self._queue.elements.items():
			print "key:", k, "value:", v

		for k,v in self._nonack_queue_index.items():
			print "Dumping nonack_queue_index #", k
			for k2 in v.keys():
				print "key:", k2

		print "------End Dump ----------"
		self.connection.transaction_manager.commit()

	def _rollback_unacknowledged_items(self):
		for k,nonack_queue in self._nonack_queue_index.items():
			keys = []
			for item in nonack_queue.keys():
				keys.append(item)

			for item in keys:
				self._put_in_queue_head(self._queue, item)
				nonack_queue.remove(item)
				

	def connect(self, is_first = False): # we need to lock the connect with a specific mutex since self.index can be changed by more than one thread.
		self.connect_mutex.acquire()

		if is_first:
			new_queue = self
			self.shared_data['connections_counter'] = 0
			self.shared_data['disconnects_counter'] = 0
			self.queue_index = 0
			new_queue.connection = new_queue.db.open()
		else:
			new_queue = copy.copy(self)
			new_queue.shared_data['connections_counter'] += 1
			new_queue.queue_index = new_queue.shared_data['connections_counter']
			tran = transaction.TransactionManager()
			new_queue.connection = new_queue.db.open(tran)

		new_queue.root = new_queue.connection.root()
		new_queue.connection.transaction_manager.begin()

		if is_first:
			if 'queue' not in new_queue.root:
				new_queue.root['queue'] = PersistentDeque()
				new_queue.root['nonack_queue_index'] = IOBTree()

		new_queue._queue = new_queue.root["queue"]
		new_queue._nonack_queue_index = new_queue.root["nonack_queue_index"]

		if is_first:
			self._rollback_unacknowledged_items()

		new_deque = OOTreeSet()
		new_queue._nonack_queue = new_deque
		new_queue._nonack_queue_index[new_queue.queue_index] = new_deque

		new_queue.connection.transaction_manager.commit()

		self.connect_mutex.release()

		return new_queue


	def _remove_from_queue_with_index(self, queue, idx):
		del queue.elements[idx]
		if idx == queue.start: # minimum optimization of space. The btree indexed by idx is not the more efficient implementation for storing non acknowledged items.
			queue.start += 1
		elif idx == queue.stop - 1:
			queue.stop -= 1

		if queue.start == queue.stop: # reset to 0. Only for cosmetic.
			queue.start = 0
			queue.stop = 0
			


	def ack(self, item): # don't need blocking since ack only affects the TreeSet on one thread.
		self.connection.transaction_manager.begin()
		self._nonack_queue.remove(item)
		self.connection.transaction_manager.commit()

	def _destructor(self): # if thread finishes or crashes it inserts the items again in the queue
		self.connection.transaction_manager.begin()
		keys = []
		for item in self._nonack_queue.keys(): # we need to add items first to remove them later.
			keys.append(item)

		for item in keys:
			self._put_in_queue_head(self._queue, item)
			self._nonack_queue.remove(item)
		self.connection.transaction_manager.commit()
		self.connection.close()

		self.connect_mutex.acquire()
		if self.shared_data['disconnects_counter'] == self.shared_data['connections_counter']:
			self.db.close()
			self.storage.close()
		else:
			self.shared_data['disconnects_counter'] += 1
		self.connect_mutex.release()
	
	def __del__(self): # borrowed from Queue.put, converted assuming block=True and timeout=None
		self.not_full.acquire()
		try:
			if self.maxsize > 0:
				while self._qsize() == self.maxsize:
						self.not_full.wait()
			self._destructor()
			self.not_empty.notify()
		finally:
			self.not_full.release()

	def task_done(self):
		raise NotImplementedError
