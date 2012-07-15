"""
obo.py
Provides lightweight parser of OBO ontology files. Uses an in-memory, or
optionally on-disk, SQLite3 db to handle reasoning, and can reason arbitrarily
over given predicate relationships.
"""
import sqlite3
from collections import defaultdict


TERM_SCHEMA_SQL = """CREATE TABLE terms(id, name, def, comment, synonym,
    unique(id) on conflict replace)"""


GRAPH_SCHEMA_SQL = """CREATE TABLE graph(subject, predicate, object,
    unique(subject, predicate, object) on conflict replace
    foreign key(subject) references terms(id))"""


INSERT_SQL = """INSERT INTO terms(id, name, def)
VALUES(?, ?, ?)"""


INSERT_GRAPH_SQL = """INSERT INTO graph(subject, predicate, object)
VALUES(?, ?, ?)"""


DESC_SQL = """SELECT subject FROM graph WHERE predicate like ?
AND object like ?"""


ASC_SQL = """SELECT object FROM graph WHERE predicate like ?
AND subject like ?"""


MULTIVALUE = ['is_a', 'relationship']


def _parse(line):
    """Returns a line split on a ': ' as a key/value."""
    if ':' in line:
        key, val = line.split(': ', 1)
        val = val.strip('\n ')
        return key, val
    else:
        return line, None


def _strip_comments(string):
    """Removes comments (text after ' ! ')."""
    if ' ! ' in string:
        val = string.split('!', 1)
        val = val[0].strip()
        return val
    else:
        return string


def _parse_obo(obofile):
    """Yields stanzas as encountered in OBO file as dicts."""
    stanza = None
    kvals = defaultdict(list)
    for line in obofile:
        if stanza:
            yield stanza
        key, val = _parse(line)
        if val:
            kvals[key].append(_strip_comments(val))
        else:
            for key, val in kvals.iteritems():
                kvals[key] = val[0] if (len(val) == 1
                    and key not in MULTIVALUE) else val
            stanza = kvals
            kvals = defaultdict(list)


def _parse_relationship(string):
    """Parses the values from the relationship key.
    Ex: 'part_of GO:0001234 ! some go term' => ('part_of', 'GO:0001234')"""
    string = _strip_comments(string)
    pred, obj = string.split(' ', 1)
    return pred.strip(), obj.strip()


class Obo:
    """Ontology created from an OBO file specified during construction. Use
    Obo.query() to reason over relationships, and Obo.show_relations() to see
    available relationships.
    """
    conn = None

    def __init__(self, obofile=None, dbname=':memory:', new=True):
        """Instantiates a new Obo object, by default using an in-memory db.
        If obofile given, imports into db. If dbname given, uses on-disk db."""
        self._createdb(dbname, new)
        if obofile:
            for term in _parse_obo(obofile):
                self._ins_stanza(term)

    def _createdb(self, dbname, new):
        """Creates a connection to the specified SQLite db."""
        self.conn = sqlite3.connect(dbname)
        if new:
            self.conn.execute(TERM_SCHEMA_SQL)
            self.conn.execute(GRAPH_SCHEMA_SQL)

    def _ins_stanza(self, stanza):
        """Inserts a stanza into the db, accounting for is_a and other
        relationships."""
        required_fields = set(('id', 'name', 'def'))
        if required_fields.issubset(stanza.keys()):
            if 'is_obsolete' in stanza and stanza['is_obsolete']:
                return
            self.conn.execute(INSERT_SQL,
                        (stanza['id'],
                         stanza['name'].decode('utf-8'),
                         stanza['def'].decode('utf-8')))
        if 'is_a' in stanza:
            for related in stanza['is_a']:
                self.conn.execute(INSERT_GRAPH_SQL,
                    (stanza['id'], 'is_a', related))
        if 'relationship' in stanza:
            for related in stanza['relationship']:
                relationship, target = _parse_relationship(related)
                self.conn.execute(INSERT_GRAPH_SQL,
                    (stanza['id'], relationship, target))

    def find_children(self, _id, expand=False):
        """Returns children (using 'is_a' relation) of given id.
        If expand=True, returns all [great,...] grandchildren."""
        return self.query(_id, 'is_a', expand=expand)

    def find_parents(self, _id, expand=False):
        """Returns parents (using 'is_a' relation) of given id.
        If expand=True, returns all [great,...] grandparents."""
        return self.query(_id, 'is_a', desc=False, expand=expand)

    def query(self, _id, predicate, desc=True, expand=True):
        """Returns all the descendants of a given predicate relationship, or
        if desc=False, returns ascendants. If expand=True, traverses up or
        down tree to terminal nodes."""
        related = []
        sql = DESC_SQL if desc else ASC_SQL
        cur = self.conn.execute(sql, (predicate, _id))
        rset = cur.fetchall()
        cur.close()
        for relative in rset:
            related.append(relative[0])
            if expand:
                for sub in self.query(relative[0], predicate, desc, expand):
                    related.append(sub)
        return related

    def show_relations(self):
        """Shows all the predicate relationships from the graph."""
        cur = self.conn.execute("select distinct predicate from graph")
        rset = [x[0] for x in cur.fetchall()]
        cur.close()
        return rset
