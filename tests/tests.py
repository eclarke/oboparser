"""
unit tests for obo.py.
"""
import urllib
import obo


goslim = ("http://cvsweb.geneontology.org/cgi-bin/cvsweb.cgi/go/"
    "GO_slims/goslim_generic.obo?rev=1.1006;content-type=text%2Fplain")


def test_create():
    f = urllib.urlopen(goslim)
    o = obo.Obo(f)
    f.close()
    children = o.find_children('GO:0006810')
    assert children == [u'GO:0006605', u'GO:0006913', u'GO:0007034',
        u'GO:0016192', u'GO:0030705', u'GO:0055085']
