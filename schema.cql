CREATE KEYSPACE IF NOT EXISTS kai_ai WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS kai_ai.freebase_tuple (id int, lhs list<int>, predicate int, rhs list<int>, PRIMARY KEY (id));

CREATE TABLE IF NOT EXISTS kai_ai.freebase_index (predicate int, word int, tuples list<int>, PRIMARY KEY ((predicate, word)));

CREATE TABLE IF NOT EXISTS kai_ai.freebase_vocab (id int, word text, is_predicate boolean, PRIMARY KEY (id));

CREATE TABLE IF NOT EXISTS kai_ai.freebase_word (word text, id int, is_predicate boolean, PRIMARY KEY (word));
