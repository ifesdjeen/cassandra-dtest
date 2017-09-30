# coding: utf-8

import itertools
import struct
import time

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.metadata import NetworkTopologyStrategy, SimpleStrategy
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement

from dtest import ReusableClusterTester, debug, Tester, create_ks
from distutils.version import LooseVersion
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_tests import get_thrift_client
from tools.assertions import (assert_all, assert_invalid, assert_length_equal,
                              assert_none, assert_one, assert_unavailable)
from tools.decorators import since
from tools.metadata_wrapper import (UpdatingClusterMetadataWrapper,
                                    UpdatingKeyspaceMetadataWrapper,
                                    UpdatingTableMetadataWrapper)


from cql_tests import CQLTester

class NoCompactStorageTest(CQLTester):
    def test_sparse_compact(self):
        session = self.prepare(nodes=2, rf=2)
        node1 = self.cluster.nodelist()[0]
        session.execute("CREATE TABLE sparse_compact_table (k int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE;")
        nc_session = self.patient_cql_connection(node1, no_compact=True)
        nc_session.execute("USE ks;")

        # Populate
        for i in range(1, 5):
            nc_session.execute("INSERT INTO sparse_compact_table (k, column1, v1, v2, value) VALUES ({i}, 'a{i}',    {i},    {i},    textAsBlob('b{i}'))".format(i = i))
            nc_session.execute("INSERT INTO sparse_compact_table (k, column1, v1, v2, value) VALUES ({i}, 'a{i}{i}', {i}{i}, {i}{i}, textAsBlob('b{i}{i}'))".format(i = i))

        assert_all(nc_session, "SELECT * FROM sparse_compact_table",
                   [[1, u'a1', 11, 11, 'b1'],
                    [1, u'a11', 11, 11, 'b11'],
                    [2, u'a2', 22, 22, 'b2'],
                    [2, u'a22', 22, 22, 'b22'],
                    [3, u'a3', 33, 33, 'b3'],
                    [3, u'a33', 33, 33, 'b33'],
                    [4, u'a4', 44, 44, 'b4'],
                    [4, u'a44', 44, 44, 'b44']],
                   ignore_order=True)

        assert_all(session, "SELECT * FROM sparse_compact_table",
                   [[1, 11, 11],
                    [2, 22, 22],
                    [3, 33, 33],
                    [4, 44, 44]],
                   ignore_order=True)

        assert_all(nc_session, "SELECT * FROM sparse_compact_table WHERE k=1",
                   [[1, u'a1', 11, 11, 'b1'],
                    [1, u'a11', 11, 11, 'b11']],
                   ignore_order=True)

        assert_all(session, "SELECT * FROM sparse_compact_table WHERE k=1",
                   [[1, 11, 11]],
                   ignore_order=True)

        assert_all(nc_session, "SELECT * FROM sparse_compact_table WHERE k=1 AND column1='a1'",
                   [[1, u'a1', 11, 11, 'b1']],
                   ignore_order=True)

        # UPDATE
        nc_session.execute("UPDATE sparse_compact_table SET value = textAsBlob('updated') WHERE k=1 AND column1='a1'",)
        assert_all(nc_session, "SELECT * FROM sparse_compact_table WHERE k=1 AND column1='a1'",
                   [[1, u'a1', 11, 11, 'updated']],
                   ignore_order=True)

        # DELETE
        nc_session.execute("DELETE FROM sparse_compact_table WHERE k=1",)
        assert_all(nc_session, "SELECT * FROM sparse_compact_table",
                   [[2, u'a2', 22, 22, 'b2'],
                    [2, u'a22', 22, 22, 'b22'],
                    [3, u'a3', 33, 33, 'b3'],
                    [3, u'a33', 33, 33, 'b33'],
                    [4, u'a4', 44, 44, 'b4'],
                    [4, u'a44', 44, 44, 'b44']],
                   ignore_order=True)

        nc_session.execute("DELETE FROM sparse_compact_table WHERE k=2 AND column1='a22'",)
        assert_all(nc_session, "SELECT * FROM sparse_compact_table",
                   [[2, u'a2', 22, 22, 'b2'],
                    [3, u'a3', 33, 33, 'b3'],
                    [3, u'a33', 33, 33, 'b33'],
                    [4, u'a4', 44, 44, 'b4'],
                    [4, u'a44', 44, 44, 'b44']],
                   ignore_order=True)

    def test_dense_compact(self):
        session = self.prepare(nodes=2, rf=2)
        session.execute("CREATE TABLE dense_compact_table (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;")
        node1 = self.cluster.nodelist()[0]
        nc_session = self.patient_cql_connection(node1, no_compact=True)
        nc_session.execute("USE ks;")

        # Populate
        for i in range(1, 5):
            nc_session.execute("INSERT INTO dense_compact_table (pk, ck, v1) VALUES ({i}, {i}, {i})".format(i = i))
            nc_session.execute("INSERT INTO dense_compact_table (pk, ck, v1) VALUES ({i}, {i}{i}, {i}{i})".format(i = i))

        assert_all(nc_session, "SELECT * FROM dense_compact_table",
                   [[1, 1, 1],
                    [1, 11, 11],
                    [2, 2, 2],
                    [2, 22, 22],
                    [3, 3, 3],
                    [3, 33, 33],
                    [4, 4, 4],
                    [4, 44, 44]],
                   ignore_order=True)

        assert_all(session, "SELECT * FROM dense_compact_table",
                   [[1, 1, 1],
                    [1, 11, 11],
                    [2, 2, 2],
                    [2, 22, 22],
                    [3, 3, 3],
                    [3, 33, 33],
                    [4, 4, 4],
                    [4, 44, 44]],
                   ignore_order=True)

        assert_all(nc_session, "SELECT * FROM dense_compact_table where pk=1",
                   [[1, 1, 1],
                    [1, 11, 11]],
                   ignore_order=True)

        assert_all(nc_session, "SELECT * FROM dense_compact_table where pk=1 AND ck=11",
                   [[1, 11, 11]],
                   ignore_order=True)

        # UPDATE
        nc_session.execute("UPDATE dense_compact_table SET v1 = 100 WHERE pk=1 AND ck=1",)
        assert_all(nc_session, "SELECT * FROM dense_compact_table WHERE pk=1 AND ck=1",
                   [[1, 1, 100]],
                   ignore_order=True)

        # DELETE
        nc_session.execute("DELETE FROM dense_compact_table WHERE pk=1",)
        assert_all(nc_session, "SELECT * FROM dense_compact_table",
                   [[2, 2, 2],
                    [2, 22, 22],
                    [3, 3, 3],
                    [3, 33, 33],
                    [4, 4, 4],
                    [4, 44, 44]],
                   ignore_order=True)

        nc_session.execute("DELETE FROM dense_compact_table WHERE pk=2 AND ck=22",)
        assert_all(nc_session, "SELECT * FROM dense_compact_table",
                   [[2, 2, 2],
                    [3, 3, 3],
                    [3, 33, 33],
                    [4, 4, 4],
                    [4, 44, 44]],
                   ignore_order=True)

    def test_dense_compact_without_value_columns(self):
        session = self.prepare(nodes=2, rf=2)
        session.execute("CREATE TABLE dense_compact_table (pk int, ck int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE;")
        node1 = self.cluster.nodelist()[0]
        nc_session = self.patient_cql_connection(node1, no_compact=True)
        nc_session.execute("USE ks;")

        # Populate
        for i in range(1, 5):
            nc_session.execute("INSERT INTO dense_compact_table (pk, ck) VALUES ({i}, {i})".format(i = i))
            nc_session.execute("INSERT INTO dense_compact_table (pk, ck) VALUES ({i}, {i}{i})".format(i = i))

        # Value is an EmptyType, so would accept only null as values
        assert_all(nc_session, "SELECT * FROM dense_compact_table",
                   [[1, 1, None],
                    [1, 11, None],
                    [2, 2, None],
                    [2, 22, None],
                    [3, 3, None],
                    [3, 33, None],
                    [4, 4, None],
                    [4, 44, None]],
                   ignore_order=True)

        assert_all(nc_session, "SELECT * FROM dense_compact_table where pk=1 AND ck=11",
                   [[1, 11, None]],
                   ignore_order=True)
