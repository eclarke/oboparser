import sqlite3
from collections import defaultdict

TERM_SCHEMA_SQL = """CREATE TABLE terms(id, name, def, comment, synonym,
    unique(id) on conflict replace)"""


HEIR_SCHEMA_SQL = """CREATE TABLE heirarchy(id, rel, related,
    unique(id, rel, related) on conflict replace
    foreign key(id) references terms(id))"""


INSERT_SQL = """INSERT INTO terms(id, name, def)
VALUES(?, ?, ?)"""


INSERT_HEIR_SQL = """INSERT INTO heirarchy(id, rel, related)
VALUES(?, ?, ?)"""


CHILDREN_SQL = """SELECT id FROM heirarchy WHERE rel like 'is_a'
AND related like ?"""


PARENT_SQL = """SELECT related FROM heirarchy WHERE rel like 'is_a'
AND id like ?"""


VALID_RELATIONSHIPS = ['is_a', 'relationship']


class Obo:

    conn = None

    def __init__(self, obofile):
        self.conn = self._createdb()
        for term in self._parse_obo(obofile):
            self._ins_stanza(term)

    def _createdb(self):
        conn = sqlite3.connect(':memory:')
        conn.execute(TERM_SCHEMA_SQL)
        conn.execute(HEIR_SCHEMA_SQL)
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
                        and k not in VALID_RELATIONSHIPS) else v
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
                self.conn.execute(INSERT_HEIR_SQL,
                    (stanza['id'], 'is_a', related))
        if 'relationship' in stanza:
            for related in stanza['relationship']:
                relationship, target = self._parse_relationship(related)
                self.conn.execute(INSERT_HEIR_SQL,
                    (stanza['id'], relationship, target))

    def _find_related(self, relation, goid, infer):
        related = []
        sql = CHILDREN_SQL if relation == 'child' else PARENT_SQL
        c = self.conn.execute(sql, (goid,))
        rset = c.fetchall()
        c.close()
        for relative in rset:
            related.append(relative[0])
            if infer:
                for sub in self._find_related(relation, relative[0], infer):
                    related.append(sub)
        return related

    def find_children(self, goid, infer=False):
        return self._find_related("child", goid, infer)

    def find_parents(self, goid, infer=False):
        return self._find_related("parent", goid, infer)
