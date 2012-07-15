from argparse import ArgumentParser
import obo
import json
import urllib


def main():
    parser = ArgumentParser(
        description='Returns relatives of a term from an ontology.')
    parser.add_argument('--obo', action='store', dest='obo',
        help='A local .obo (v1.2) file to use')
    parser.add_argument('--remote_obo', action='store', dest='remote_obo',
        help='A remote .obo (v1.2) file to use')
    parser.add_argument('--db', action='store', dest='db',
        help='Store the db in the local file specified, instead of in memory')
    parser.add_argument('--children', action='store_true', dest='children',
        help='Return the children of the specified term(s)')
    parser.add_argument('--parents', action='store_true', dest='parents',
        help='Return the parents of the specified term(s)')
    parser.add_argument('terms', action='store', nargs='+',
        help='Term or terms to process')
    parser.add_argument('--expand', action='store_true', dest='expand',
        default=False, help='Expand search up/down the graph')
    args = parser.parse_args()

    if args.obo:
        with open(args.obo) as obof:
            ontology = obo.Obo(obof, dbname=args.db)
    elif args.remote_obo:
        obof = urllib.urlopen(args.remote_obo)
        ontology = obo.Obo(obof, dbname=args.db)
        obof.close()
    elif args.db:
        ontology = obo.Obo(dbname=args.db)
    else:
        parser.error("Must specify either a pre-existing db or .obo file "
            "to use as reference.")

    results = {}
    find = ontology.find_children if args.children else ontology.find_parents
    for term in args.terms:
        results[term] = find(term, expand=args.expand)
    print json.dumps(results)


if __name__ == '__main__':
    main()
