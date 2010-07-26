# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# to run a single test, run from trunk/:
# PYTHONPATH=test nosetests --tests=system.test_thrift_server:TestMutations.test_empty_range

import os, sys, time, struct

from . import root, ThriftTester
from . import thrift_client as client

from thrift.Thrift import TApplicationException
from ttypes import *
from constants import VERSION


def _i64(n):
    return struct.pack('>q', n) # big endian = network order

_SIMPLE_COLUMNS = [Column('c1', 'value1', Clock(0)),
                   Column('c2', 'value2', Clock(0))]
_SUPER_COLUMNS = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', Clock(0))]),
                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', Clock(0)),
                                                   Column(_i64(6), 'value6', Clock(0))])]

def _assert_column(column_family, key, column, value, ts = 0):
    try:
        assert client.get(key, ColumnPath(column_family, column=column), ConsistencyLevel.ONE).column == Column(column, value, Clock(ts))
    except NotFoundException:
        raise Exception('expected %s:%s:%s:%s, but was not present' % (column_family, key, column, value) )

def _assert_columnpath_exists(key, column_path):
    try:
        assert client.get(key, column_path, ConsistencyLevel.ONE)
    except NotFoundException:
        raise Exception('expected %s with %s but was not present.' % (key, column_path) )

def _assert_no_columnpath(key, column_path):
    try:
        client.get(key, column_path, ConsistencyLevel.ONE)
        assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
    except NotFoundException:
        assert True, 'column did not exist'

def _insert_simple(block=True):
   return _insert_multi(['key1'], block)

def _insert_batch(block):
   return _insert_multi_batch(['key1'], block)

def _insert_multi(keys, block=True):
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO

    for key in keys:
        client.insert(key, ColumnParent('Standard1'), Column('c1', 'value1', Clock(0)), consistencyLevel)
        client.insert(key, ColumnParent('Standard1'), Column('c2', 'value2', Clock(0)), consistencyLevel)

def _insert_multi_batch(keys, block):
    cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS],
             'Standard2': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]}
    if block:
        consistencyLevel = ConsistencyLevel.ONE
    else:
        consistencyLevel = ConsistencyLevel.ZERO

    for key in keys:
        client.batch_mutate({key: cfmap}, consistencyLevel)

def _big_slice(key, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.get_slice(key, column_parent, p, ConsistencyLevel.ONE)

def _big_multislice(keys, column_parent):
    p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
    return client.multiget_slice(keys, column_parent, p, ConsistencyLevel.ONE)

def _verify_batch():
    _verify_simple()
    L = [result.column
         for result in _big_slice('key1', ColumnParent('Standard2'))]
    assert L == _SIMPLE_COLUMNS, L

def _verify_simple():
    assert client.get('key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE).column == Column('c1', 'value1', Clock(0))
    L = [result.column
         for result in _big_slice('key1', ColumnParent('Standard1'))]
    assert L == _SIMPLE_COLUMNS, L

def _insert_super(key='key1'):
    client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', Clock(0)), ConsistencyLevel.ZERO)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(0)), ConsistencyLevel.ZERO)
    client.insert(key, ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', Clock(0)), ConsistencyLevel.ZERO)
    time.sleep(0.1)

def _insert_range():
    client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', Clock(0)), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c2', 'value2', Clock(0)), ConsistencyLevel.ONE)
    client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', Clock(0)), ConsistencyLevel.ONE)
    time.sleep(0.1)

def _verify_range():
    p = SlicePredicate(slice_range=SliceRange('c1', 'c2', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c1'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('c3', 'c2', True, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == 'c3'
    assert result[1].column.name == 'c2'

    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 1000))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result
    
    p = SlicePredicate(slice_range=SliceRange('a', 'z', False, 2))
    result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result

def _set_keyspace(keyspace):
    client.set_keyspace(keyspace)

def _insert_super_range():
    client.insert('key1', ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', Clock(0)), False)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(0)), False)
    client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(6), 'value6', Clock(0)), False)
    client.insert('key1', ColumnParent('Super1', 'sc3'), Column(_i64(7), 'value7', Clock(0)), False)
    time.sleep(0.1)

def _verify_super_range():
    p = SlicePredicate(slice_range=SliceRange('sc2', 'sc3', False, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc2'
    assert result[1].super_column.name == 'sc3'

    p = SlicePredicate(slice_range=SliceRange('sc3', 'sc2', True, 2))
    result = client.get_slice('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == 'sc3'
    assert result[1].super_column.name == 'sc2'

def _verify_super(supercf='Super1', key='key1'):
    assert client.get(key, ColumnPath(supercf, 'sc1', _i64(4)), ConsistencyLevel.ONE).column == Column(_i64(4), 'value4', Clock(0))
    slice = [result.super_column
             for result in _big_slice(key, ColumnParent('Super1'))]
    assert slice == _SUPER_COLUMNS, slice

def _expect_exception(fn, type_):
    try:
        r = fn()
    except type_, t:
        return t
    else:
        raise Exception('expected %s; got %s' % (type_.__name__, r))
    
def _expect_missing(fn):
    _expect_exception(fn, NotFoundException)

def waitfor(secs, fn, *args, **kwargs):
    start = time.time()
    success = False
    last_exception = None
    while not success and time.time() < start + secs:
        try:
            fn(*args, **kwargs)
            success = True
        except KeyboardInterrupt:
            raise
        except Exception, e:
            last_exception = e
            pass
    if not success and last_exception:
        raise last_exception

def get_range_slice(client, parent, predicate, start, end, count, cl):
    kr = KeyRange(start, end, count=count)
    return client.get_range_slices(parent, predicate, kr, cl)
    

ZERO_WAIT = 5

class TestMutations(ThriftTester):
    def test_insert(self):
        _set_keyspace('Keyspace1')
        _insert_simple(False)
        time.sleep(0.1)
        _verify_simple()

    def test_empty_slice(self):
        _set_keyspace('Keyspace1')
        assert _big_slice('key1', ColumnParent('Standard2')) == []
        assert _big_slice('key1', ColumnParent('Super1')) == []

    def test_missing_super(self):
        _set_keyspace('Keyspace1')
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))
        _insert_super()
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1', _i64(1)), ConsistencyLevel.ONE))

    def test_count(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        assert client.get_count('key1', ColumnParent('Standard2'), p, ConsistencyLevel.ONE) == 0
        assert client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 2
        assert client.get_count('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE) == 2
        assert client.get_count('key1', ColumnParent('Super1'), p, ConsistencyLevel.ONE) == 2

        # Let's make that a little more interesting
        client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('c4', 'value4', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('c5', 'value5', Clock(0)), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange('c2', 'c4', False, 1000)) 
        assert client.get_count('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 3

    def test_insert_blocking(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _verify_simple()

    def test_super_insert(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        _verify_super()

    def test_super_get(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        result = client.get('key1', ColumnPath('Super1', 'sc2'), ConsistencyLevel.ONE).super_column
        assert result == _SUPER_COLUMNS[1], result

    def test_super_subcolumn_limit(self):
        _set_keyspace('Keyspace1')
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc2')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(5), 'value5', Clock(0))], slice
        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(6), 'value6', Clock(0))], slice
        
    def test_long_order(self):
        _set_keyspace('Keyspace1')
        def long_xrange(start, stop, step):
            i = start
            while i < stop:
                yield i
                i += step
        L = []
        for i in long_xrange(0, 104294967296, 429496729):
            name = _i64(i)
            client.insert('key1', ColumnParent('StandardLong1'), Column(name, 'v', Clock(0)), ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice('key1', ColumnParent('StandardLong1'))]
        assert slice == L, slice
        
    def test_time_uuid(self):
        import uuid
        L = []
        _set_keyspace('Keyspace2')
        # 100 isn't enough to fail reliably if the comparator is borked
        for i in xrange(500):
            L.append(uuid.uuid1())
            client.insert('key1', ColumnParent('Super4', 'sc1'), Column(L[-1].bytes, 'value%s' % i, Clock(i)), ConsistencyLevel.ONE)
        slice = _big_slice('key1', ColumnParent('Super4', 'sc1'))
        assert len(slice) == 500, len(slice)
        for i in xrange(500):
            u = slice[i].column
            assert u.value == 'value%s' % i
            assert u.name == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange('', '', True, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[-1].bytes, 'value499', Clock(499))], slice

        p = SlicePredicate(slice_range=SliceRange('', L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[0].bytes, 'value0', Clock(0)),
                         Column(L[1].bytes, 'value1', Clock(1)),
                         Column(L[2].bytes, 'value2', Clock(2))], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', True, 1000))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', Clock(2)),
                         Column(L[1].bytes, 'value1', Clock(1)),
                         Column(L[0].bytes, 'value0', Clock(0))], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, '', False, 1))
        column_parent = ColumnParent('Super4', 'sc1')
        slice = [result.column
                 for result in client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, 'value2', Clock(2))], slice
        
    def test_long_remove(self):
        column_parent = ColumnParent('StandardLong1')
        sp = SlicePredicate(slice_range=SliceRange('', '', False, 1))
        _set_keyspace('Keyspace1')
        for i in xrange(10):
            parent = ColumnParent('StandardLong1')

            client.insert('key1', parent, Column(_i64(i), 'value1', Clock(10 * i)), ConsistencyLevel.ONE)
            client.remove('key1', ColumnPath('StandardLong1'), Clock(10 * i + 1), ConsistencyLevel.ONE)
            slice = client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert('key1', parent, Column(_i64(i), 'value2', Clock(10 * i + 2)), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice('key1', column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), 'value2', Clock(10 * i + 2))], (slice, i)
        
    def test_batch_insert(self):
        _set_keyspace('Keyspace1')
        _insert_batch(False)
        time.sleep(0.1)
        _verify_batch()

    def test_batch_insert_blocking(self):
        _set_keyspace('Keyspace1')
        _insert_batch(True)
        _verify_batch()
        
    def test_batch_mutate_standard_columns(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(27,32)] 
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ZERO)

        for column_family in column_families:
            for key in keys:
               waitfor(ZERO_WAIT, _assert_column, column_family, key, 'c1', 'value1')

    def test_batch_mutate_standard_columns_blocking(self):
        _set_keyspace('Keyspace1')
        
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in  range(38,46)]
         
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)
        
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column(column_family, key, 'c1', 'value1')

    def test_batch_mutate_remove_standard_columns(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(Clock(20), predicate=SlicePredicate(column_names=[c.name]))) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_standard_row(self):
        _set_keyspace('Keyspace1')
        column_families = ['Standard1', 'Standard2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(Clock(20)))]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_super_columns_with_standard_under(self):
        _set_keyspace('Keyspace1')
        column_families = ['Super1', 'Super2']
        keys = ['key_%d' % i for i in range(11,21)]
        _insert_super()

        mutations = []
        for sc in _SUPER_COLUMNS:
            names = []
            for c in sc.columns:
                names.append(c.name)
            mutations.append(Mutation(deletion=Deletion(Clock(20), super_column=c.name, predicate=SlicePredicate(column_names=names))))

        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ZERO)
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc.columns:
                    for key in keys:
                        waitfor(ZERO_WAIT, _assert_no_columnpath, key, ColumnPath(column_family, super_column=sc.name, column=c.name))

    def test_batch_mutate_remove_super_columns_with_none_given_underneath(self):
        _set_keyspace('Keyspace1')
        
        keys = ['key_%d' % i for i in range(17,21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        for sc in _SUPER_COLUMNS:
            mutations.append(Mutation(deletion=Deletion(Clock(20),
                                                        super_column=sc.name)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ZERO)

        for sc in _SUPER_COLUMNS:
            for c in sc.columns:
                for key in keys:
                    waitfor(ZERO_WAIT, _assert_no_columnpath, key, ColumnPath('Super1', super_column=sc.name))
    
    def test_batch_mutate_remove_super_columns_entire_row(self):
        _set_keyspace('Keyspace1')
        
        keys = ['key_%d' % i for i in range(17,21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        mutations.append(Mutation(deletion=Deletion(Clock(20))))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ZERO)

        for sc in _SUPER_COLUMNS:
          for key in keys:
            waitfor(ZERO_WAIT, _assert_no_columnpath, key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_insertions_and_deletions(self):
        _set_keyspace('Keyspace1')
        
        first_insert = SuperColumn("sc1",
                                   columns=[Column(_i64(20), 'value20', Clock(3)),
                                            Column(_i64(21), 'value21', Clock(3))])
        second_insert = SuperColumn("sc1",
                                    columns=[Column(_i64(20), 'value20', Clock(3)),
                                             Column(_i64(21), 'value21', Clock(3))])
        first_deletion = {'super_column': "sc1",
                          'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}
        second_deletion = {'super_column': "sc2",
                           'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}

        keys = ['key_30', 'key_31']
        for key in keys:
            sc = SuperColumn('sc1',[Column(_i64(22), 'value22', Clock(0)),
                                    Column(_i64(23), 'value23', Clock(0))])
            cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=sc))]}
            client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)

            sc2 = SuperColumn('sc2', [Column(_i64(22), 'value22', Clock(0)),
                                      Column(_i64(23), 'value23', Clock(0))])
            cfmap2 = {'Super2': [Mutation(ColumnOrSuperColumn(super_column=sc2))]}
            client.batch_mutate({key: cfmap2}, ConsistencyLevel.ONE)

        cfmap3 = {
            'Super1' : [Mutation(ColumnOrSuperColumn(super_column=first_insert)),
                        Mutation(deletion=Deletion(Clock(3), **first_deletion))],
        
            'Super2' : [Mutation(deletion=Deletion(Clock(2), **second_deletion)),
                        Mutation(ColumnOrSuperColumn(super_column=second_insert))]
            }

        keyed_mutations = dict((key, cfmap3) for key in keys)
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for key in keys:
            for c in [_i64(22), _i64(23)]:
                _assert_no_columnpath(key, ColumnPath('Super1', super_column='sc1', column=c))
                _assert_no_columnpath(key, ColumnPath('Super2', super_column='sc2', column=c))

            for c in [_i64(20), _i64(21)]:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column='sc1', column=c))
                _assert_columnpath_exists(key, ColumnPath('Super2', super_column='sc1', column=c))

    def test_batch_mutate_does_not_accept_cosc_and_deletion_in_same_mutation(self):
        def too_full():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', Clock(0)))
            dele = Deletion(Clock(2), predicate=SlicePredicate(column_names=['baz']))
            client.batch_mutate({'key_34': {'Standard1': [Mutation(col, dele)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(too_full, InvalidRequestException)

    def test_batch_mutate_does_not_yet_accept_slice_ranges(self):
        def send_range():
            _set_keyspace('Keyspace1')
            sp = SlicePredicate(slice_range=SliceRange(start='0', finish="", count=10))
            d = Deletion(Clock(2), predicate=sp)
            client.batch_mutate({'key_35': {'Standard1':[Mutation(deletion=d)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(send_range, InvalidRequestException)

    def test_batch_mutate_does_not_accept_cosc_on_undefined_cf(self):
        def bad_cf():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column("foo", 'bar', Clock(0)))
            client.batch_mutate({'key_36': {'Undefined': [Mutation(col)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(bad_cf, InvalidRequestException)

    def test_batch_mutate_does_not_accept_deletion_on_undefined_cf(self):
        def bad_cf():
            _set_keyspace('Keyspace1')
            d = Deletion(Clock(2), predicate=SlicePredicate(column_names=['baz']))
            client.batch_mutate({'key_37': {'Undefined':[Mutation(deletion=d)]}},
                                 ConsistencyLevel.ONE)
        _expect_exception(bad_cf, InvalidRequestException)

    def test_column_name_lengths(self):
        _set_keyspace('Keyspace1')
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), Column('', 'value', Clock(0)), ConsistencyLevel.ONE), InvalidRequestException)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*1, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*127, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*128, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*129, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*255, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*256, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*257, 'value', Clock(0)), ConsistencyLevel.ONE)
        client.insert('key1', ColumnParent('Standard1'), Column('x'*(2**16 - 1), 'value', Clock(0)), ConsistencyLevel.ONE)
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), Column('x'*(2**16), 'value', Clock(0)), ConsistencyLevel.ONE), InvalidRequestException)

    def test_bad_calls(self):
        _set_keyspace('Keyspace1')
        # missing arguments
        _expect_exception(lambda: client.insert(None, None, None, None), TApplicationException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1', 'x'), Column('y', 'value', Clock(0)), ConsistencyLevel.ONE), InvalidRequestException)
        # key too long
        _expect_exception(lambda: client.get('x' * 2**16, ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        # empty key
        _expect_exception(lambda: client.get('', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE), InvalidRequestException)
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS]}
        _expect_exception(lambda: client.batch_mutate({'': cfmap}, ConsistencyLevel.ONE), InvalidRequestException)
        # empty column name
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1', column=''), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify column name
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1'), ConsistencyLevel.ONE), InvalidRequestException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.get('key1', ColumnPath('Standard1', 'x', 'y'), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify supercolumn name
        _expect_exception(lambda: client.get('key1', ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        # invalid CF
        _expect_exception(lambda: get_range_slice(client, ColumnParent('S'), SlicePredicate(column_names=['', '']), '', '', 5, ConsistencyLevel.ONE), InvalidRequestException)
        # 'x' is not a valid Long
        _expect_exception(lambda: client.insert('key1', ColumnParent('Super1', 'sc1'), Column('x', 'value', Clock(0)), ConsistencyLevel.ONE), InvalidRequestException)
        # start is not a valid Long
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start is not a valid Long, supercolumn version
        p = SlicePredicate(slice_range=SliceRange('x', '', False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, supercolumn version
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('Super1', 'sc1')
        _expect_exception(lambda: client.get_slice('key1', column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, key version
        _expect_exception(lambda: get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['']), 'z', 'a', 1, ConsistencyLevel.ONE), InvalidRequestException)
        # ttl must be positive
        column = Column('cttl1', 'value1', Clock(0), 0)
        _expect_exception(lambda: client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # don't allow super_column in Deletion for standard ColumnFamily
        deletion = Deletion(Clock(1), 'supercolumn', None)
        mutation = Mutation(deletion=deletion)
        mutations = {'key' : {'Standard1' : [mutation]}}
        _expect_exception(lambda: client.batch_mutate(mutations, ConsistencyLevel.QUORUM),
                          InvalidRequestException)

    def test_batch_insert_super(self):
         _set_keyspace('Keyspace1')
         cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS],
                  'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS]}
         client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ZERO)
         waitfor(ZERO_WAIT, _verify_super, 'Super1')
         waitfor(ZERO_WAIT, _verify_super, 'Super2')

    def test_batch_insert_super_blocking(self):
         _set_keyspace('Keyspace1')
         cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) 
                             for c in _SUPER_COLUMNS],
                  'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                             for c in _SUPER_COLUMNS]}
         client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
         _verify_super('Super1')
         _verify_super('Super2')

    def test_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        client.remove('key1', ColumnPath('Standard1', column='c1'), Clock(1), ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='c1'), ConsistencyLevel.ONE))
        assert client.get('key1', ColumnPath('Standard1', column='c2'), ConsistencyLevel.ONE).column \
            == Column('c2', 'value2', Clock(0))
        assert _big_slice('key1', ColumnParent('Standard1')) \
            == [ColumnOrSuperColumn(column=Column('c2', 'value2', Clock(0)))]

        # New insert, make sure it shows up post-remove:
        client.insert('key1', ColumnParent('Standard1'), Column('c3', 'value3', Clock(0)), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', Clock(0)), Column('c3', 'value3', Clock(0))], columns

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', Clock(0)), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c2', 'value2', Clock(0)), Column('c3', 'value3', Clock(0))], columns
        # Next, w/ a newer timestamp; it should come back:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', Clock(2)), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice('key1', ColumnParent('Standard1'))]
        assert columns == [Column('c1', 'value1', Clock(2)), Column('c2', 'value2', Clock(0)), Column('c3', 'value3', Clock(0))], columns


    def test_cf_remove(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf; verify super is unaffected
        client.remove('key1', ColumnPath('Standard1'), Clock(3), ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Standard1')) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', Clock(0)), ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Standard1')) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert('key1', ColumnParent('Standard1'), Column('c1', 'value1', Clock(4)), ConsistencyLevel.ONE)
        result = _big_slice('key1', ColumnParent('Standard1'))
        assert result == [ColumnOrSuperColumn(column=Column('c1', 'value1', Clock(4)))], result

        # check removing the entire super cf, too.
        client.remove('key1', ColumnPath('Super1'), Clock(3), ConsistencyLevel.ONE)
        assert _big_slice('key1', ColumnParent('Super1')) == []
        assert _big_slice('key1', ColumnParent('Super1', 'sc1')) == []


    def test_super_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('key1', ColumnPath('Super1', 'sc2', _i64(5)), Clock(5), ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', Clock(0))]),
                                 SuperColumn(name='sc2', columns=[Column(_i64(6), 'value6', Clock(0))])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(7), 'value7', Clock(0)), ConsistencyLevel.ONE)
        super_columns_expected = [SuperColumn(name='sc1', 
                                              columns=[Column(_i64(4), 'value4', Clock(0))]),
                                  SuperColumn(name='sc2', 
                                              columns=[Column(_i64(6), 'value6', Clock(0)), Column(_i64(7), 'value7', Clock(0))])]

        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, actual

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(0)), ConsistencyLevel.ONE)

        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(6)), ConsistencyLevel.ONE)
        super_columns = [result.super_column for result in _big_slice('key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', Clock(0))]), 
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', Clock(6)), 
                                                                   Column(_i64(6), 'value6', Clock(0)), 
                                                                   Column(_i64(7), 'value7', Clock(0))])]
        assert super_columns == super_columns_expected, super_columns

    def test_super_cf_remove_supercolumn(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove('key1', ColumnPath('Super1', 'sc2'), Clock(5), ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc2', _i64(5)), ConsistencyLevel.ONE))
        super_columns = _big_slice('key1', ColumnParent('Super1', 'sc2'))
        assert super_columns == [], super_columns
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', Clock(0))])]
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns
        _verify_simple()

        # Test resurrection.  First, re-insert the value w/ older timestamp, 
        # and make sure it stays removed:
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(1)), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(5), 'value5', Clock(6)), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice('key1', ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name='sc1', columns=[Column(_i64(4), 'value4', Clock(0))]),
                                  SuperColumn(name='sc2', columns=[Column(_i64(5), 'value5', Clock(6))])]
        assert super_columns == super_columns_expected, super_columns

        # check slicing at the subcolumn level too
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        columns = [result.column
                   for result in client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)]
        assert columns == [Column(_i64(5), 'value5', Clock(6))], columns


    def test_super_cf_resurrect_subcolumn(self):
        _set_keyspace('Keyspace1')
        key = 'vijay'
        client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', Clock(0)), ConsistencyLevel.ONE)

        client.remove(key, ColumnPath('Super1', 'sc1'), Clock(1), ConsistencyLevel.ONE)

        client.insert(key, ColumnParent('Super1', 'sc1'), Column(_i64(4), 'value4', Clock(2)), ConsistencyLevel.ONE)

        result = client.get(key, ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE)
        assert result.super_column.columns is not None, result.super_column


    def test_empty_range(self):
        _set_keyspace('Keyspace1')
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Super1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE) == []

    def test_range_with_remove(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('key1', ColumnPath('Standard1', column='c1'), Clock(1), ConsistencyLevel.ONE)
        client.remove('key1', ColumnPath('Standard1', column='c2'), Clock(1), ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c2']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_with_remove_cf(self):
        _set_keyspace('Keyspace1')
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), 'key1', '', 1000, ConsistencyLevel.ONE)[0].key == 'key1'

        client.remove('key1', ColumnPath('Standard1'), Clock(1), ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['c1', 'c1']), '', '', 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key='key1')], actual

    def test_range_collation(self):
        _set_keyspace('Keyspace1')
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert(key, ColumnParent('Standard1'), Column(key, 'v', Clock(0)), ConsistencyLevel.ONE)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '', 1000, ConsistencyLevel.ONE)
        # note the collated ordering rather than ascii
        L = ['0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27','28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'a', '-a', 'b', '-b']
        assert len(slices) == len(L)
        for key, ks in zip(L, slices):
            assert key == ks.key

    def test_range_partial(self):
        _set_keyspace('Keyspace1')
        
        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in xrange(100)]:
            client.insert(key, ColumnParent('Standard1'), Column(key, 'v', Clock(0)), ConsistencyLevel.ONE)

        def check_slices_against_keys(keyList, sliceList):
            assert len(keyList) == len(sliceList)
            for key, ks in zip(keyList, sliceList):
                assert key == ks.key
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), 'a', '', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['a', '-a', 'b', '-b'], slices)
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '', '15', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['0', '1', '10', '11', '12', '13', '14', '15'], slices)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '50', '51', 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['50', '51'], slices)
        
        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=['-a', '-a']), '1', '', 10, ConsistencyLevel.ONE)
        check_slices_against_keys(['1', '10', '11', '12', '13', '14', '15', '16', '17', '18'], slices)

    def test_get_slice_range(self):
        _set_keyspace('Keyspace1')
        _insert_range()
        _verify_range()
        
    def test_get_slice_super_range(self):
        _set_keyspace('Keyspace1')
        _insert_super_range()
        _verify_super_range()

    def test_get_range_slices_tokens(self):
        _set_keyspace('Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Super3', 'sc1'), Column(cname, 'v-' + cname, Clock(0)), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        predicate = SlicePredicate(column_names=['col1', 'col3'])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

    def test_get_range_slice_super(self):
        _set_keyspace('Keyspace2')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Super3', 'sc1'), Column(cname, 'v-' + cname, Clock(0)), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', 'sc1')
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        cp = ColumnParent('Super3')
        result = get_range_slice(client, cp, SlicePredicate(column_names=['sc1']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert list(set(row.columns[0].super_column.name for row in result))[0] == 'sc1'
        
    def test_get_range_slice(self):
        _set_keyspace('Keyspace1')
        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, Clock(0)), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        # test empty slice
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key6', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 0

        # test empty columns
        result = get_range_slice(client, cp, SlicePredicate(column_names=['a']), 'key2', '', 1, ConsistencyLevel.ONE)
        assert len(result) == 1
        assert len(result[0].columns) == 0

        # test column_names predicate
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 5, ConsistencyLevel.ONE)
        assert len(result) == 3, result
        assert result[0].columns[0].column.name == 'col1'
        assert result[0].columns[1].column.name == 'col3'

        # row limiting via count.
        result = get_range_slice(client, cp, SlicePredicate(column_names=['col1', 'col3']), 'key2', 'key4', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # test column slice predicate
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].key == 'key1'
        assert result[1].key == 'key2'
        assert len(result[0].columns) == 3
        assert result[0].columns[0].column.name == 'col2'
        assert result[0].columns[2].column.name == 'col4'

        # col limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=2)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 2

        # and reversed 
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col4', finish='col2', reversed=True, count=5)), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert result[0].columns[0].column.name == 'col4'
        assert result[0].columns[2].column.name == 'col2'

        # row limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start='col2', finish='col4', reversed=False, count=5)), 'key1', 'key2', 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # removed data
        client.remove('key1', ColumnPath('Standard1', column='col1'), Clock(1), ConsistencyLevel.ONE)
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange('', '')), 'key1', 'key2', 5, ConsistencyLevel.ONE)
        assert len(result) == 2, result
        assert result[0].columns[0].column.name == 'col2', result[0].columns[0].column.name
        assert result[1].columns[0].column.name == 'col1'
        
    
    def test_wrapped_range_slices(self):
        _set_keyspace('Keyspace1')

        def copp_token(key):
            # I cheated and generated this from Java
            return {'a': '00530000000100000001', 
                    'b': '00540000000100000001', 
                    'c': '00550000000100000001',
                    'd': '00560000000100000001', 
                    'e': '00580000000100000001'}[key]

        for key in ['a', 'b', 'c', 'd', 'e']:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                client.insert(key, ColumnParent('Standard1'), Column(cname, 'v-' + cname, Clock(0)), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        result = client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('e'), end_token=copp_token('e')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['a', 'b', 'c', 'd', 'e',], [row.key for row in result]

        result = client.get_range_slices(cp, SlicePredicate(column_names=['col1', 'col3']), KeyRange(start_token=copp_token('c'), end_token=copp_token('c')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == ['d', 'e', 'a', 'b', 'c',], [row.key for row in result]
        

    def test_get_slice_by_names(self):
        _set_keyspace('Keyspace1')
        _insert_range()
        p = SlicePredicate(column_names=['c1', 'c2'])
        result = client.get_slice('key1', ColumnParent('Standard1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 2
        assert result[0].column.name == 'c1'
        assert result[1].column.name == 'c2'

        _insert_super()
        p = SlicePredicate(column_names=[_i64(4)])
        result = client.get_slice('key1', ColumnParent('Super1', 'sc1'), p, ConsistencyLevel.ONE) 
        assert len(result) == 1
        assert result[0].column.name == _i64(4)

    def test_multiget_slice(self):
        """Insert multiple keys and retrieve them using the multiget_slice interface"""

        _set_keyspace('Keyspace1')
        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        _insert_multi(keys)

        # Retrieve all 10 key slices
        rows = _big_multislice(keys, ColumnParent('Standard1'))
        keys1 = rows.keys().sort()
        keys2 = keys.sort()

        columns = [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert rows.has_key(key) == True
            assert columns == rows[key]

    def test_multi_count(self):
        """Insert multiple keys and count them using the multiget interface"""
        _set_keyspace('Keyspace1')

        # Generate a list of 10 keys countaining 1 to 10 columns and insert them
        num_keys = 10
        for i in range(1, num_keys+1):
          key = 'key'+str(i)
          for j in range(1, i+1):
            client.insert(key, ColumnParent('Standard1'), Column('c'+str(j), 'value'+str(j), Clock(0)), ConsistencyLevel.ONE)

        # Count columns in all 10 keys
        keys = ['key'+str(i) for i in range(1, num_keys+1)]
        p = SlicePredicate(slice_range=SliceRange('', '', False, 1000))
        counts = client.multiget_count('Keyspace1', keys, ColumnParent('Standard1'), p, ConsistencyLevel.ONE)

        # Check the returned counts
        for i in range(1, num_keys+1):
          key = 'key'+str(i)
          assert counts[key] == i

    def test_batch_mutate_super_deletion(self):
        _set_keyspace('Keyspace1')
        _insert_super('test')
        d = Deletion(Clock(1), predicate=SlicePredicate(column_names=['sc1']))
        cfmap = {'Super1': [Mutation(deletion=d)]}
        client.batch_mutate({'test': cfmap}, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Super1', 'sc1'), ConsistencyLevel.ONE))

    def test_super_reinsert(self):
        _set_keyspace('Keyspace1')
        for x in xrange(3):
            client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x), 'value', Clock(1)), ConsistencyLevel.ONE)

        client.remove('key1', ColumnPath('Super1'), Clock(2), ConsistencyLevel.ONE)

        for x in xrange(3):
            client.insert('key1', ColumnParent('Super1', 'sc2'), Column(_i64(x + 3), 'value', Clock(3)), ConsistencyLevel.ONE)

        for n in xrange(1, 4):
            p =  SlicePredicate(slice_range=SliceRange('', '', False, n))
            slice = client.get_slice('key1', ColumnParent('Super1', 'sc2'), p, ConsistencyLevel.ONE)
            assert len(slice) == n, "expected %s results; found %s" % (n, slice)

    def test_describe_keyspace(self):
        """ Test keyspace description """
        kspaces = client.describe_keyspaces()
        assert len(kspaces) == 5, kspaces # ['system', 'Keyspace2', 'Keyspace3', 'Keyspace1', 'Keyspace4']
        ks1 = client.describe_keyspace("Keyspace1")
        assert set(ks1.keys()) == set(['Super1', 'Standard1', 'Standard2', 'StandardLong1', 'StandardLong2', 'Super3', 'Super2', 'Super4', 'IncrementCounter1', 'SuperIncrementCounter1', 'Indexed1'])
        sysks = client.describe_keyspace("system")

    def test_describe(self):
        server_version = client.describe_version()
        assert server_version == VERSION, (server_version, VERSION)
        assert client.describe_cluster_name() == 'Test Cluster'

    def test_describe_ring(self):
        assert list(client.describe_ring('Keyspace1'))[0].endpoints == ['127.0.0.1']
    
    def test_system_keyspace_operations(self):
        """ Test keyspace (add, drop, rename) operations """
        # create
        keyspace = KsDef('CreateKeyspace', 'org.apache.cassandra.locator.RackUnawareStrategy', 1,
                         [CfDef('CreateKeyspace', 'CreateKsCf')])
        client.system_add_keyspace(keyspace)
        newks = client.describe_keyspace('CreateKeyspace')
        assert 'CreateKsCf' in newks
        
        _set_keyspace('CreateKeyspace')
        
        # rename
        client.system_rename_keyspace('CreateKeyspace', 'RenameKeyspace')
        renameks = client.describe_keyspace('RenameKeyspace')
        assert 'CreateKsCf' in renameks
        def get_first_ks():
            client.describe_keyspace('CreateKeyspace')
        _expect_exception(get_first_ks, NotFoundException)
        
        # drop
        client.system_drop_keyspace('RenameKeyspace')
        def get_second_ks():
            client.describe_keyspace('RenameKeyspace')
        _expect_exception(get_second_ks, NotFoundException)

    def test_column_validators(self):
        ks = 'Keyspace1'
        _set_keyspace(ks)
        cd = ColumnDef('col', 'LongType', None, None)
        cf = CfDef('Keyspace1', 'ValidatorColumnFamily', column_metadata=[cd])
        client.system_add_column_family(cf)
        dks = client.describe_keyspace(ks)
        assert 'ValidatorColumnFamily' in dks

        cp = ColumnParent('ValidatorColumnFamily')
        col0 = Column('col', _i64(42), Clock(0))
        col1 = Column('col', "ceci n'est pas 64bit", Clock(0))
        client.insert('key0', cp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: client.insert('key1', cp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

    def test_system_column_family_operations(self):
        _set_keyspace('Keyspace1')
        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' in ks1
        
        # rename
        client.system_rename_column_family('NewColumnFamily', 'RenameColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'RenameColumnFamily' in ks1
        assert 'NewColumnFamily' not in ks1
        
        # drop
        client.system_drop_column_family('RenameColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'RenameColumnFamily' not in ks1
        assert 'NewColumnFamily' not in ks1
        assert 'Standard1' in ks1

    def test_system_super_column_family_operations(self):
        _set_keyspace('Keyspace1')
        
        # create
        cd = ColumnDef('ValidationColumn', 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewSuperColumnFamily', 'Super', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' in ks1
        
        # rename
        client.system_rename_column_family('NewSuperColumnFamily', 'RenameSuperColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'RenameSuperColumnFamily' in ks1
        assert 'NewSuperColumnFamily' not in ks1
        
        # drop
        client.system_drop_column_family('RenameSuperColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'RenameSuperColumnFamily' not in ks1
        assert 'NewSuperColumnFamily' not in ks1
        assert 'Standard1' in ks1

    def test_insert_ttl(self):
        """ Test simple insertion of a column with ttl """
        _set_keyspace('Keyspace1')
        column = Column('cttl1', 'value1', Clock(0), 5)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        assert client.get('key1', ColumnPath('Standard1', column='cttl1'), ConsistencyLevel.ONE).column == column

    def test_simple_expiration(self):
        """ Test that column ttled do expires """
        _set_keyspace('Keyspace1')
        column = Column('cttl3', 'value1', Clock(0), 2)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        time.sleep(1)
        c = client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE).column
        assert c == column
        assert client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE).column == column
        time.sleep(2)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE))
    
    def test_simple_expiration_batch_mutate(self):
        """ Test that column ttled do expires using batch_mutate """
        _set_keyspace('Keyspace1')
        column = Column('cttl4', 'value1', Clock(0), 2)
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(column))]}
        client.batch_mutate({'key1': cfmap}, ConsistencyLevel.ONE)
        time.sleep(1)
        c = client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column
        assert c == column
        assert client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column == column
        time.sleep(2)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='cttl3'), ConsistencyLevel.ONE))

    def test_update_expiring(self):
        """ Test that updating a column with ttl override the ttl """
        _set_keyspace('Keyspace1')
        column1 = Column('cttl4', 'value1', Clock(0), 1)
        client.insert('key1', ColumnParent('Standard1'), column1, ConsistencyLevel.ONE)
        column2 = Column('cttl4', 'value1', Clock(1))
        client.insert('key1', ColumnParent('Standard1'), column2, ConsistencyLevel.ONE)
        time.sleep(1.5)
        assert client.get('key1', ColumnPath('Standard1', column='cttl4'), ConsistencyLevel.ONE).column == column2

    def test_remove_expiring(self):
        """ Test removing a column with ttl """
        _set_keyspace('Keyspace1')
        column = Column('cttl5', 'value1', Clock(0), 10)
        client.insert('key1', ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        client.remove('key1', ColumnPath('Standard1', column='cttl5'), Clock(1), ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get('key1', ColumnPath('Standard1', column='ctt5'), ConsistencyLevel.ONE))
    
    def test_describe_ring_on_invalid_keyspace(self):
        def req():
            client.describe_ring('system')
        _expect_exception(req, InvalidRequestException)

    def test_incr_standard_insert(self):
        d1 = 12
        d2 = 21
        d3 = 35
        d1p = struct.pack('>q', d1)
        d2p = struct.pack('>q', d2)
        d3p = struct.pack('>q', d3)
        _set_keyspace('Keyspace1')
        # insert positive values and check the counts
        client.insert('key1', ColumnParent('IncrementCounter1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath('IncrementCounter1', column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv1.column.value)[0] == d1

        client.insert('key1', ColumnParent('IncrementCounter1'), Column('c1', d2p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath('IncrementCounter1', column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv2.column.value)[0] == (d1+d2)

        client.insert('key1', ColumnParent('IncrementCounter1'), Column('c1', d3p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = client.get('key1', ColumnPath('IncrementCounter1', column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv3.column.value)[0] == (d1+d2+d3)

    def test_incr_super_insert(self):
        d1 = 234
        d2 = 52345
        d3 = 3123
        d1p = struct.pack('>q', d1)
        d2p = struct.pack('>q', d2)
        d3p = struct.pack('>q', d3)
        _set_keyspace('Keyspace1')
        client.insert('key1', ColumnParent('SuperIncrementCounter1', 'sc1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv1.column.value)[0] == d1

        client.insert('key1', ColumnParent('SuperIncrementCounter1', 'sc1'), Column('c1', d2p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv2.column.value)[0] == (d1+d2)

        client.insert('key1', ColumnParent('SuperIncrementCounter1', 'sc1'), Column('c1', d3p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = client.get('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv3.column.value)[0] == (d1+d2+d3)

    def test_incr_standard_remove(self):
        d1 = 124
        d1p = struct.pack('>q', d1)

        _set_keyspace('Keyspace1')
        # insert value and check it exists
        client.insert('key1', ColumnParent('IncrementCounter1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath('IncrementCounter1', column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv1.column.value)[0] == d1

        # remove the previous column and check that it is gone
        client.remove('key1', ColumnPath('IncrementCounter1', column='c1'), Clock(), ConsistencyLevel.ONE)
        time.sleep(0.1)
        _assert_no_columnpath('key1', ColumnPath('IncrementCounter1', column='c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.insert('key1', ColumnParent('IncrementCounter1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath('IncrementCounter1', column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv2.column.value)[0] == d1
        client.remove('key1', ColumnPath('IncrementCounter1'), Clock(), ConsistencyLevel.ONE)
        time.sleep(0.1)
        _assert_no_columnpath('key1', ColumnPath('IncrementCounter1', column='c1'))

    def test_incr_super_remove(self):
        d1 = 52345
        d1p = struct.pack('>q', d1)

        _set_keyspace('Keyspace1')
        # insert value and check it exists
        client.insert('key1', ColumnParent('SuperIncrementCounter1', 'sc1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv1.column.value)[0] == d1

        # remove the previous column and check that it is gone
        client.remove('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), Clock(), ConsistencyLevel.ONE)
        time.sleep(0.1)
        _assert_no_columnpath('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'))

        # insert again and this time delete the whole row, check that it is gone
        client.insert('key1', ColumnParent('SuperIncrementCounter1', 'sc1'), Column('c1', d1p, Clock()), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv2.column.value)[0] == d1
        client.remove('key1', ColumnPath('SuperIncrementCounter1', 'sc1'), Clock(), ConsistencyLevel.ONE)
        time.sleep(0.1)
        _assert_no_columnpath('key1', ColumnPath('SuperIncrementCounter1', 'sc1', 'c1'))

    def test_batch_mutate_increment_columns_blocking(self):
        d1 = 234
        d2 = 52345
        _set_keyspace('Keyspace1')
        cf = 'IncrementCounter1'
        key = 'key1'
        values = [struct.pack('>q', d1), struct.pack('>q', d2)]
        
        mutations = [Mutation(ColumnOrSuperColumn(Column('c1', v, Clock()))) for v in values]
        mutation_map = dict({cf: mutations})
        keyed_mutations = dict({key: mutation_map})
        
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)
        #print struct.unpack('>q', client.get(key, ColumnPath(cf, column='c1'), ConsistencyLevel.ONE).column.value)[0]
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)
        #print struct.unpack('>q', client.get(key, ColumnPath(cf, column='c1'), ConsistencyLevel.ONE).column.value)[0]
        
        time.sleep(0.1)
        
        rv = client.get(key, ColumnPath(cf, column='c1'), ConsistencyLevel.ONE)
        assert struct.unpack('>q', rv.column.value)[0] == (d1+d1+d2+d2)      

    def test_index_scan(self):
        _set_keyspace('Keyspace1')
        client.insert('key1', ColumnParent('Indexed1'), Column('birthdate', _i64(1), Clock(0)), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('Indexed1'), Column('birthdate', _i64(2), Clock(0)), ConsistencyLevel.ONE)
        client.insert('key2', ColumnParent('Indexed1'), Column('b', _i64(2), Clock(0)), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('Indexed1'), Column('birthdate', _i64(3), Clock(0)), ConsistencyLevel.ONE)
        client.insert('key3', ColumnParent('Indexed1'), Column('b', _i64(3), Clock(0)), ConsistencyLevel.ONE)

        # simple query on one index expression
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange('', ''))
        clause = IndexClause([IndexExpression('birthdate', IndexOperator.EQ, _i64(1))])
        result = client.scan(cp, RowPredicate(index_clause=clause), sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key1'
        assert len(result[0].columns) == 1, result[0].columns

        # solo unindexed expression is invalid
        clause = IndexClause([IndexExpression('b', IndexOperator.EQ, _i64(1))])
        _expect_exception(lambda: client.scan(cp, RowPredicate(index_clause=clause), sp, ConsistencyLevel.ONE), InvalidRequestException)

        # but unindexed expression added to indexed one is ok
        clause = IndexClause([IndexExpression('b', IndexOperator.EQ, _i64(3)),
                              IndexExpression('birthdate', IndexOperator.EQ, _i64(3))])
        result = client.scan(cp, RowPredicate(index_clause=clause), sp, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == 'key3'
        assert len(result[0].columns) == 2, result[0].columns
        

class TestTruncate(ThriftTester):
    def test_truncate(self):
        _set_keyspace('Keyspace1')
        
        _insert_simple()
        _insert_super()

        # truncate Standard1
        client.truncate('Standard1')
        assert _big_slice('key1', ColumnParent('Standard1')) == []

        # truncate Super1
        client.truncate('Super1')
        assert _big_slice('key1', ColumnParent('Super1')) == []
        assert _big_slice('key1', ColumnParent('Super1', 'sc1')) == []
