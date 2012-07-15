=========
obo.py
=========

Installation
----------
obo.py has no external dependencies, so installation is trivial.

Usage
----------
In scripts:: 
    from obo import Obo
    import urllib
    f = urllib.urlopen('http://geneontology.org/GO_slims/goslim_generic.obo')

    # optionally, use Obo(f, dbname='some.db') to use on-disk db.
    # if 'some.db' already exists, the .obo file can be omitted.
    o = Obo(f)

    # Find all children of the Biological Process term
    print o.find_children('GO:0008150', expand=True)

From the command line::
    $ python reasoner.py --remote_obo 'http://geneontology.org/GO_slims/goslim_generic.obo' GO:0008150

Read the docstrings in obo.py, or ``python reasoner.py -h`` for more 
information. It's pretty basic and has been tested with OBO format 1.2
files from the Gene Ontology and Human Disease Ontology.

Author
---------
erikclarke@gmail.com
