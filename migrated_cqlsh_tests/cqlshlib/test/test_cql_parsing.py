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

# to configure behavior, define $CQL_TEST_HOST to the destination address
# and $CQL_TEST_PORT to the associated port.

from unittest import TestCase
from operator import itemgetter

from ..cql3handling import CqlRuleSet


class TestCqlParsing(TestCase):
    def test_parse_string_literals(self):
        for n in ["'eggs'", "'Sausage 1'", "'spam\nspam\n\tsausage'", "''"]:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'quotedStringLiteral')])
        self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex("'eggs'")),
                                 [("'eggs'", 'quotedStringLiteral')])

        tokens = CqlRuleSet.lex("'spam\nspam\n\tsausage'")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[0][0], "quotedStringLiteral")

        tokens = CqlRuleSet.lex("'spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[0][0], "unclosedString")

        tokens = CqlRuleSet.lex("'foo bar' 'spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[1][0], "unclosedString")

    def test_parse_pgstring_literals(self):
        for n in ["$$eggs$$", "$$Sausage 1$$", "$$spam\nspam\n\tsausage$$", "$$$$"]:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'pgStringLiteral')])
        self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex("$$eggs$$")),
                                 [("$$eggs$$", 'pgStringLiteral')])

        tokens = CqlRuleSet.lex("$$spam\nspam\n\tsausage$$")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [(b'pgStringLiteral', '$$spam\nspam\n\tsausage$$', (0, 22))]
        self.assertEqual(tokens[0][0], "pgStringLiteral")

        tokens = CqlRuleSet.lex("$$spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [(b'unclosedPgString', '$$', (0, 2)), (b'identifier', 'spam', (2, 6)), (b'identifier', 'spam', (7, 11))]
        self.assertEqual(tokens[0][0], "unclosedPgString")

        tokens = CqlRuleSet.lex("$$foo bar$$ $$spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [(b'pgStringLiteral', '$$foo bar$$', (0, 11)), (b'unclosedPgString', '$$', (12, 14)), (b'identifier', 'spam', (14, 18)), (b'identifier', 'spam', (19, 23))]
        self.assertEqual(tokens[0][0], "pgStringLiteral")
        self.assertEqual(tokens[1][0], "unclosedPgString")

    def test_parse_numbers(self):
        for n in ['6', '398', '18018']:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'wholenumber')])

    def test_parse_uuid(self):
        uuids = ['4feeae80-e9cc-11e4-b571-0800200c9a66',
                 '7142303f-828f-4806-be9e-7a973da0c3f9',
                 'dff8d435-9ca0-487c-b5d0-b0fe5c5768a8']
        for u in uuids:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(u)),
                                     [(u, 'uuid')])

    def test_comments_in_string_literals(self):
        comment_strings = ["'sausage -- comment'",
                           "'eggs and spam // comment string'",
                           "'spam eggs sausage and spam /* still in string'"]
        for s in comment_strings:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(s)),
                                     [(s, 'quotedStringLiteral')])

    def test_colons_in_string_literals(self):
        comment_strings = ["'Movie Title: The Movie'",
                           "':a:b:c:'",
                           "'(>>=) :: (Monad m) => m a -> (a -> m b) -> m b'"]
        for s in comment_strings:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(s)),
                                     [(s, 'quotedStringLiteral')])

    def test_partial_parsing(self):
        [parsed] = CqlRuleSet.cql_parse('INSERT INTO ks.test')
        self.assertSequenceEqual(parsed.matched, [])
        self.assertSequenceEqual(tokens_with_types(parsed.remainder),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'test', 'identifier')])

    def test_parse_select(self):
        parsed = parse_cqlsh_statements('SELECT FROM ks.tab;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'SELECT', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements('SELECT FROM "MyTable";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'SELECT', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'"MyTable"', 'quotedName'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE foo = 3;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'SELECT', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'foo', 'identifier'),
                                  (b'=', 'op'),
                                  (b'3', 'wholenumber'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab ORDER BY event_id DESC LIMIT 1000')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'SELECT', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'ORDER', 'reserved_identifier'),
                                  (b'BY', 'reserved_identifier'),
                                  (b'event_id', 'identifier'),
                                  (b'DESC', 'reserved_identifier'),
                                  (b'LIMIT', 'reserved_identifier'),
                                  (b'1000', 'wholenumber')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE clustering_column > 200 '
            'AND clustering_column < 400 ALLOW FILTERING')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'SELECT', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'clustering_column', 'identifier'),
                                  (b'>', 'cmp'),
                                  (b'200', 'wholenumber'),
                                  (b'AND', 'reserved_identifier'),
                                  (b'clustering_column', 'identifier'),
                                  (b'<', 'cmp'),
                                  (b'400', 'wholenumber'),
                                  # 'allow' and 'filtering' are not keywords
                                  (b'ALLOW', 'reserved_identifier'),
                                  (b'FILTERING', 'identifier')])

    def test_parse_insert(self):
        parsed = parse_cqlsh_statements('INSERT INTO mytable (x) VALUES (2);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'mytable', 'identifier'),
                                  (b'(', 'op'),
                                  (b'x', 'identifier'),
                                  (b')', 'op'),
                                  (b'VALUES', 'identifier'),
                                  (b'(', 'op'),
                                  (b'2', 'wholenumber'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'mytable', 'identifier'),
                                  (b'(', 'op'),
                                  (b'x', 'identifier'),
                                  (b',', 'op'),
                                  (b'y', 'identifier'),
                                  (b')', 'op'),
                                  (b'VALUES', 'identifier'),
                                  (b'(', 'op'),
                                  (b'2', 'wholenumber'),
                                  (b',', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'mytable', 'identifier'),
                                  (b'(', 'op'),
                                  (b'x', 'identifier'),
                                  (b',', 'op'),
                                  (b'y', 'identifier'),
                                  (b')', 'op'),
                                  (b'VALUES', 'identifier'),
                                  (b'(', 'op'),
                                  (b'2', 'wholenumber'),
                                  (b',', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (ids) VALUES "
            "(7ee251da-af52-49a4-97f4-3f07e406c7a7) "
            "USING TTL 86400;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'mytable', 'identifier'),
                                  (b'(', 'op'),
                                  (b'ids', 'identifier'),
                                  (b')', 'op'),
                                  (b'VALUES', 'identifier'),
                                  (b'(', 'op'),
                                  (b'7ee251da-af52-49a4-97f4-3f07e406c7a7', 'uuid'),
                                  (b')', 'op'),
                                  (b'USING', 'reserved_identifier'),
                                  (b'TTL', 'identifier'),
                                  (b'86400', 'wholenumber'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO test_table (username) VALUES ('Albert') "
            "USING TIMESTAMP 1240003134 AND TTL 600;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'INSERT', 'reserved_identifier'),
                                  (b'INTO', 'reserved_identifier'),
                                  (b'test_table', 'identifier'),
                                  (b'(', 'op'),
                                  (b'username', 'identifier'),
                                  (b')', 'op'),
                                  (b'VALUES', 'identifier'),
                                  (b'(', 'op'),
                                  (b"'Albert'", 'quotedStringLiteral'),
                                  (b')', 'op'),
                                  (b'USING', 'reserved_identifier'),
                                  (b'TIMESTAMP', 'identifier'),
                                  (b'1240003134', 'wholenumber'),
                                  (b'AND', 'reserved_identifier'),
                                  (b'TTL', 'identifier'),
                                  (b'600', 'wholenumber'),
                                  (b';', 'endtoken')])

    def test_parse_update(self):
        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab USING TTL 432000 SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'USING', 'reserved_identifier'),
                                  (b'TTL', 'identifier'),
                                  (b'432000', 'wholenumber'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15, y = 'sausage' "
            "WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b',', 'op'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'sausage'", 'quotedStringLiteral'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 "
            "WHERE y IN ('eggs', 'sausage', 'spam');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'IN', 'reserved_identifier'),
                                  (b'(', 'op'),
                                  (b"'eggs'", 'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'sausage'", 'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'spam'", 'quotedStringLiteral'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 "
            "WHERE y = 'spam' IF z = 'sausage';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'spam'", 'quotedStringLiteral'),
                                  (b'IF', 'reserved_identifier'),
                                  (b'z', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'sausage'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' "
            "IF z = 'sausage' AND w = 'spam';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'spam'", 'quotedStringLiteral'),
                                  (b'IF', 'reserved_identifier'),
                                  (b'z', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'sausage'", 'quotedStringLiteral'),
                                  (b'AND', 'reserved_identifier'),
                                  (b'w', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'spam'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' IF EXISTS")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'UPDATE', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'SET', 'reserved_identifier'),
                                  (b'x', 'identifier'),
                                  (b'=', 'op'),
                                  (b'15', 'wholenumber'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'y', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'spam'", 'quotedStringLiteral'),
                                  (b'IF', 'reserved_identifier'),
                                  (b'EXISTS', 'identifier')])

    def test_parse_delete(self):
        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE songid = 444;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DELETE', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'songs', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'songid', 'identifier'),
                                  (b'=', 'op'),
                                  (b'444', 'wholenumber'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE name IN "
            "('Yellow Submarine', 'Eleanor Rigby');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DELETE', 'reserved_identifier'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'songs', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'name', 'identifier'),
                                  (b'IN', 'reserved_identifier'),
                                  (b'(', 'op'),
                                  (b"'Yellow Submarine'", 'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'Eleanor Rigby'", 'quotedStringLiteral'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE task_map ['2014-12-25'] FROM tasks WHERE user_id = 'Santa';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DELETE', 'reserved_identifier'),
                                  (b'task_map', 'identifier'),
                                  (b'[', 'brackets'),
                                  (b"'2014-12-25'", 'quotedStringLiteral'),
                                  (b']', 'brackets'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'tasks', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'user_id', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'Santa'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE my_list[0] FROM lists WHERE user_id = 'Jim';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DELETE', 'reserved_identifier'),
                                  (b'my_list', 'identifier'),
                                  (b'[', 'brackets'),
                                  (b'0', 'wholenumber'),
                                  (b']', 'brackets'),
                                  (b'FROM', 'reserved_identifier'),
                                  (b'lists', 'identifier'),
                                  (b'WHERE', 'reserved_identifier'),
                                  (b'user_id', 'identifier'),
                                  (b'=', 'op'),
                                  (b"'Jim'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

    def test_parse_batch(self):
        pass

    def test_parse_create_keyspace(self):
        parsed = parse_cqlsh_statements(
            "CREATE KEYSPACE ks WITH REPLICATION = "
            "{'class': 'SimpleStrategy', 'replication_factor': 1};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'KEYSPACE', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'WITH', 'reserved_identifier'),
                                  (b'REPLICATION', 'identifier'),
                                  (b'=', 'op'),
                                  (b'{', 'brackets'),
                                  (b"'class'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b"'SimpleStrategy'", 'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'replication_factor'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b'1', 'wholenumber'),
                                  (b'}', 'brackets'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE KEYSPACE "Cql_test_KS" WITH REPLICATION = '
            "{'class': 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2': 2};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'KEYSPACE', 'reserved_identifier'),
                                  (b'"Cql_test_KS"', 'quotedName'),
                                  (b'WITH', 'reserved_identifier'),
                                  (b'REPLICATION', 'identifier'),
                                  (b'=', 'op'),
                                  (b'{', 'brackets'),
                                  (b"'class'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b"'NetworkTopologyStrategy'",
                                   'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'dc1'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b'3', 'wholenumber'),
                                  (b',', 'op'),
                                  (b"'dc2'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b'2', 'wholenumber'),
                                  (b'}', 'brackets'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE KEYSPACE ks WITH REPLICATION = "
            "{'class': 'NetworkTopologyStrategy', 'dc1': 3} AND "
            "DURABLE_WRITES = false;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'KEYSPACE', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'WITH', 'reserved_identifier'),
                                  (b'REPLICATION', 'identifier'),
                                  (b'=', 'op'),
                                  (b'{', 'brackets'),
                                  (b"'class'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b"'NetworkTopologyStrategy'",
                                   'quotedStringLiteral'),
                                  (b',', 'op'),
                                  (b"'dc1'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b'3', 'wholenumber'),
                                  (b'}', 'brackets'),
                                  (b'AND', 'reserved_identifier'),
                                  # 'DURABLE_WRITES' is not a keyword
                                  (b'DURABLE_WRITES', 'identifier'),
                                  (b'=', 'op'),
                                  (b'false', 'identifier'),
                                  (b';', 'endtoken')])

    def test_parse_drop_keyspace(self):
        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DROP', 'reserved_identifier'),
                                  (b'KEYSPACE', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP SCHEMA ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DROP', 'reserved_identifier'),
                                  (b'SCHEMA', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE IF EXISTS "My_ks";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'DROP', 'reserved_identifier'),
                                  (b'KEYSPACE', 'reserved_identifier'),
                                  (b'IF', 'reserved_identifier'),
                                  (b'EXISTS', 'identifier'),
                                  (b'"My_ks"', 'quotedName'),
                                  (b';', 'endtoken')])

    def test_parse_create_table(self):
        pass

    def test_parse_drop_table(self):
        pass

    def test_parse_truncate(self):
        pass

    def test_parse_alter_table(self):
        pass

    def test_parse_use(self):
        pass

    def test_parse_create_index(self):
        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab (i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 ((b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab (i) IF NOT EXISTS;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 ((b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b'IF', 'reserved_identifier'),
                                  (b'NOT', 'reserved_identifier'),
                                  (b'EXISTS', 'identifier'),
                                  (b';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON tab (KEYS(i));')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 ((b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'KEYS', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab FULL(i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'FULL', 'reserved_identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE CUSTOM INDEX idx ON ks.tab (i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'CUSTOM', 'identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE INDEX idx ON ks.tab (i) USING "
            "'org.custom.index.MyIndexClass';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b'USING', 'reserved_identifier'),
                                  (b"'org.custom.index.MyIndexClass'", 'quotedStringLiteral'),
                                  (b';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE INDEX idx ON ks.tab (i) WITH OPTIONS = "
            "{'storage': '/mnt/ssd/indexes/'};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [(b'CREATE', 'reserved_identifier'),
                                  (b'INDEX', 'reserved_identifier'),
                                  (b'idx', 'identifier'),
                                  (b'ON', 'reserved_identifier'),
                                  (b'ks', 'identifier'),
                                  (b'.', 'op'),
                                  (b'tab', 'identifier'),
                                  (b'(', 'op'),
                                  (b'i', 'identifier'),
                                  (b')', 'op'),
                                  (b'WITH', 'reserved_identifier'),
                                  (b'OPTIONS', 'identifier'),
                                  (b'=', 'op'),
                                  (b'{', 'brackets'),
                                  (b"'storage'", 'quotedStringLiteral'),
                                  (b':', 'colon'),
                                  (b"'/mnt/ssd/indexes/'", 'quotedStringLiteral'),
                                  (b'}', 'brackets'),
                                  (b';', 'endtoken')])

    def test_parse_drop_index(self):
        pass

    def test_parse_select_token(self):
        pass


def parse_cqlsh_statements(text):
    '''
    Runs its argument through the sequence of parsing steps that cqlsh takes its
    input through.

    Currently does not handle batch statements.
    '''
    # based on onecmd
    statements, _ = CqlRuleSet.cql_split_statements(text)
    # stops here. For regular cql commands, onecmd just splits it and sends it
    # off to the cql engine; parsing only happens for cqlsh-specific stmts.

    return strip_final_empty_items(statements)[0]


def tokens_with_types(lexed):
    for x in lexed:
        assert len(x) > 2, lexed
    return tuple(itemgetter(1, 0)(token) for token in lexed)


def strip_final_empty_items(xs):
    '''
    Returns its a copy of argument as a list, but with any terminating
    subsequence of falsey values removed.

    >>> strip_final_empty_items([[3, 4], [5, 6, 7], [], [], [1], []])
    [[3, 4], [5, 6, 7], [], [], [1]]
    '''
    rv = list(xs)

    while rv and not rv[-1]:
        rv = rv[:-1]

    return rv
