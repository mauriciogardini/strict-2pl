#!/usr/bin/python
# -*- coding: utf-8 -*-
import re
import sys

class Operation():
    def __init__(self, transaction, action, resource):
        self.transaction = transaction
        self.action = action
        self.resource = resource

    def __str__(self):
        return 'Operation - %s - %s - %s' % (self.transaction,
            self.action, self.resource)

    def format_as_history(self, delayed=False):
        delayment_string = 'Delayed' if delayed else ''
        if not self.resource:
            return '%s%s' % (self.action, self.transaction)
        return '%s%s%s[%s]' % (delayment_string, self.action,\
            self.transaction, self.resource)

    def is_write(self):
        return self.action == 'w'

    def is_read(self):
        return self.action == 'r'

    def is_commit(self):
        return self.action == 'c'


class Transaction():
    is_growing = True

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Transaction - %s' % (self.value)


class Lock():
    def __init__(self, transaction, exclusive, resource, released=False):
        self.transaction = transaction
        self.exclusive = exclusive
        self.resource = resource
        self.released = released

    def format_as_history(self):
        operation = 'l' if not self.released else 'u'
        lock_type = 'x' if self.exclusive else 's'
        return '%s%s%s[%s]' % (operation, lock_type, self.transaction,\
            self.resource)

    def __str__(self):
        return 'Lock - %s - %s - %s' % (self.transaction,
            'exclusive' if self.exclusive else 'shared', self.resource)


class Scheduler():
    def execute(self, history):
        self.operations = []
        self.delayed_operations = []
        self.transactions = {}
        self.locks = []
        self.execution_list = []
        self.final_history = []
        self.counter = 0

        self.parse_history(history)
        print 'Original history: %s' % (history)
        self.run_operations()
        print '--------------------'

    def parse_history(self, history):
        substrings = history.split(u' ')
        for substring in substrings:
            sre_match = None
            if re.match('r(\d+)\[([a-z]+)\]', substring):
                sre_match = re.match('r(\d+)\[([a-z]+)\]', substring)
                self.operations.append(Operation(sre_match.group(1), 'r',\
                    sre_match.group(2)))
            elif re.match('w(\d+)\[([a-z]+)\]', substring):
                sre_match = re.match('w(\d+)\[([a-z]+)\]', substring)
                self.operations.append(Operation(sre_match.group(1), 'w',\
                    sre_match.group(2)))
            elif re.match('c(\d+)', substring):
                sre_match = re.match('c(\d+)', substring)
                self.operations.append(Operation(sre_match.group(1), 'c', None))
            else:
                raise Exception('Invalid input')
            if sre_match.group(1) not in self.transactions.keys():
                self.transactions[str(sre_match.group(1))] =\
                    Transaction(sre_match.group(1))

    def print_final_history(self):
        operations_text = ''
        for item in self.final_history:
            if isinstance(item, Operation) or isinstance(item, Lock):
                operations_text += '%s, ' % (item.format_as_history())
        if operations_text:
            print 'Final history: %s' % (operations_text.strip(', '))

    def has_lock(self, operation):
        for lock in self.locks:
            if lock.resource == operation.resource and\
                lock.transaction == operation.transaction and\
                ((lock.exclusive and operation.action == 'w') or\
                (not lock.exclusive and operation.action == 'r')):
                return True
        return False

    def can_lock(self, operation):
        relevant_locks = [lock for lock in self.locks\
            if lock.resource == operation.resource]
        for lock in relevant_locks:
            if not lock.exclusive:
                if lock.transaction == operation.transaction and\
                    len(relevant_locks) == 1 and operation.action == 'w':
                    return True
                elif lock.transaction != operation.transaction and\
                    operation.action == 'r':
                    return True
            return False
        return True

    def add_lock(self, operation):
        exclusive = True if operation.action == 'w' else False
        lock = Lock(operation.transaction, exclusive, operation.resource)
        self.locks.append(lock)
        self.final_history.append(lock)
        transaction = self.transactions[operation.transaction]

    def release_locks(self, transaction):
        original_locks = list(self.locks)
        self.locks[:] = [lock for lock in self.locks\
            if not lock.transaction == transaction]
        for released_lock in set(original_locks).difference(set(self.locks)):
            lock = Lock(released_lock.transaction, released_lock.exclusive,\
                released_lock.resource, True)
            self.final_history.append(lock)

    def has_deadlock(self):
        conflicts = []
        for delayed_operation in self.delayed_operations:
            for lock in self.locks:
                if delayed_operation.transaction != lock.transaction and\
                    delayed_operation.resource == lock.resource:
                        conflicts.append((delayed_operation.transaction,
                            lock.transaction))
        conflicts_copy = list(conflicts)
        for conflict in conflicts:
            for conflict_copy in conflicts_copy:
                if conflict[0] == conflict_copy[1] and\
                    conflict[1] == conflict_copy[0]:
                    return conflict
        return False

    def can_grow_transaction(self, transaction_value):
        return self.transactions[str(transaction_value)].is_growing

    def abort_transaction(self, transaction):
        self.delayed_operations[:] = [delayed_operation for delayed_operation\
            in self.delayed_operations\
            if not delayed_operation.transaction == transaction]
        self.final_history[:] = [item for item in self.final_history\
            if not item.transaction == transaction]
        self.locks[:] = [lock for lock in self.locks\
            if not lock.transaction == transaction]
        counter_decrementer = 0
        for index, operation in enumerate(self.execution_list):
            if operation.transaction == transaction and index <= self.counter:
                counter_decrementer += 1
        self.execution_list[:] = [operation for operation\
            in self.execution_list if not operation.transaction == transaction]
        self.counter -= counter_decrementer
        for operation in self.operations:
            if operation.transaction == transaction:
                self.execution_list.append(operation)
        print 'A deadlock was found. The transaction %s was aborted.' %\
            (transaction)

    def can_commit(self, transaction):
        pending_operations = []
        pending_operations[:] = [operation for operation in\
            self.delayed_operations if operation.transaction == transaction]
        return len(pending_operations) == 0

    def has_delayed_operation(self, transaction):
        return True if [operation for operation in self.delayed_operations\
            if operation.transaction == transaction] else False

    def run_delayed_operations(self):
        if self.delayed_operations:
            redelayed_operations = []
            for delayed_operation in self.delayed_operations:
                redelayed_operation = self.run_operation(delayed_operation)
                if redelayed_operation:
                    redelayed_operations.append(redelayed_operation)
            self.delayed_operations = redelayed_operations

    def run_operation(self, operation):
        if operation.is_write() or operation.is_read():
            if self.can_grow_transaction(operation.transaction):
                if self.has_lock(operation):
                    self.final_history.append(operation)
                elif self.can_lock(operation):
                    self.add_lock(operation)
                    self.final_history.append(operation)
                else:
                    return operation
            else:
                print 'The operation %s will be ignored because its '\
                    'transaction is in the shrinking phase.' %\
                    (operation.format_as_history())
        elif operation.is_commit():
            if self.can_commit(operation.transaction):
                self.final_history.append(operation)
                self.release_locks(operation.transaction)
                self.transactions[operation.transaction].is_growing = False
            else:
                print 'It is not possible to commit the transaction %s '\
                    'because there are pending operations.' %\
                    (operation.transaction)

    def run_operations(self):
        self.execution_list = list(self.operations)
        while self.counter < len(self.execution_list):
            if self.has_delayed_operation(\
                self.execution_list[self.counter].transaction):
                self.delayed_operations.append(\
                    self.execution_list[self.counter])
                print 'The operation %s was delayed.' %\
                    (self.execution_list[self.counter].format_as_history())
            else:
                operation = self.run_operation(\
                    self.execution_list[self.counter])
                if operation:
                    self.delayed_operations.append(operation)
                    print 'The operation %s was delayed.' %\
                        (operation.format_as_history())
                    deadlock = self.has_deadlock()
                    if deadlock:
                        self.abort_transaction(operation.transaction)
                self.run_delayed_operations()
            self.counter += 1
            if self.counter == len(self.execution_list):
                for delayed_operation in self.delayed_operations:
                    self.execution_list.append(delayed_operation)
                self.delayed_operations = []
        self.print_final_history()


if __name__ == '__main__':
    scheduler = Scheduler()
    print 'History with no conflicts'
    scheduler.execute('r1[x] r2[y] r1[y] c1 w2[x] c2')
    print 'History with an operation that needs to be delayed'
    scheduler.execute('r1[x] w1[x] w2[x] c1 c2')
    print 'History with a deadlock'
    scheduler.execute('r1[x] w2[y] r1[y] w2[x] c1 c2')
    print 'History with an operation that can\'t be executed'
    scheduler.execute('r1[x] r2[y] r1[y] c1 r1[x] w2[x] c2')
    print 'History with more than one operation that has to be delayed'
    scheduler.execute('r1[x] w1[x] w2[x] r2[y] w2[y] c1 c2')
