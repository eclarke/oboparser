import sqlite3
from collections import defaultdict
from optparse import OptionParser


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


class Obo:

    conn = None

    def __init__(self, obofile=None, dbname=':memory:', new=True):
        self.conn = self._createdb(dbname, new)
        if obofile:
            for term in self._parse_obo(obofile):
                self._ins_stanza(term)

    def _createdb(self, dbname, new):
        conn = sqlite3.connect(dbname)
        if new:
            conn.execute(TERM_SCHEMA_SQL)
            conn.execute(GRAPH_SCHEMA_SQL)
        return conn

    def _parse(self, line):
        """Returns a line split on a : as a key/value.
        We don't currently handle comments in values."""
        if ':' in line:
            k, v = line.split(':', 1)
            v = v.strip('\n ')
            return k, v
        else:
            return line, None

    def _parse_relationship(self, string):
        string = self._strip_comments(string)
        pred, obj = string.split(' ', 1)
        return pred.strip(), obj.strip()

    def _strip_comments(self, string):
        if ' ! ' in string:
            v, c = string.split('!', 1)
            v = v.strip()
            return v
        else:
            return string

    def _parse_obo(self, obofile):
        stanza = None
        kvals = defaultdict(list)
        for line in obofile:
            if stanza:
                yield stanza
            k, v = self._parse(line)
            if v:
                kvals[k].append(self._strip_comments(v))
            else:
                for k, v in kvals.iteritems():
                    kvals[k] = v[0] if (len(v) == 1
                        and k not in MULTIVALUE) else v
                stanza = kvals
                kvals = defaultdict(list)

    def _ins_stanza(self, stanza):
        required_fields = set(('id', 'name', 'def'))
        if required_fields.issubset(stanza.keys()):
            if 'is_obsolete' in stanza and stanza['is_obsolete']:
                return
            self.conn.execute(INSERT_SQL,
                        (stanza['id'], stanza['name'].decode('utf-8'), stanza['def'].decode('utf-8')))
        if 'is_a' in stanza:
            for related in stanza['is_a']:
                self.conn.execute(INSERT_GRAPH_SQL,
                    (stanza['id'], 'is_a', related))
        if 'relationship' in stanza:
            for related in stanza['relationship']:
                relationship, target = self._parse_relationship(related)
                self.conn.execute(INSERT_GRAPH_SQL,
                    (stanza['id'], relationship, target))

    def find_children(self, id, expand=False):
        return self.query(id, 'is_a', expand=expand)

    def find_parents(self, id, expand=False):
        return self.query(id, 'is_a', subject=False, expand=expand)

    def query(self, id, predicate, subject=True, expand=True):
        related = []
        sql = DESC_SQL if subject else ASC_SQL
        c = self.conn.execute(sql, (predicate, id))
        rset = c.fetchall()
        c.close()
        for relative in rset:
            related.append(relative[0])
            if expand:
                for sub in self.query(relative[0], predicate, subject, expand):
                    related.append(sub)
        return related

    def show_relations(self):
        c = self.conn.execute("select distinct predicate from graph")
        rset = [x[0] for x in c.fetchall()]
        c.close()
        return rset

def main():
    parser = OptionParser()
    parser.add_option("")

if __name__ == '__main__':
    main()
