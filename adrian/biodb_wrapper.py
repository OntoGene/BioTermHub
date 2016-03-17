__author__ = 'vicawil'

import os

#from unified_builder import RecordSetContainer
from settings import dpath

HERE = os.path.dirname(__file__)


def get_resources():
    resource_dict = {}
    with open(os.path.join(HERE, 'resources'), 'r') as resources:
        for line in resources:
            key, value = line.split('\t')
            resource_dict[key] = value.rstrip()

    return resource_dict

def resource_paths(*args):
    resources = get_resources()
    rsc_args = {}

    for arg in args:
        if arg == 'ctd_lookup':
            rsc_args[arg] = True
        rsc_arg = resources.get(arg)
        if rsc_arg:
            rsc_args[arg] = dpath + rsc_arg
        elif arg == 'mesh':
            rsc_args['mesh'] = (dpath + resources['mesh_desc'],
                                dpath + resources['mesh_supp'])
    return rsc_args

#def ub_wrapper(*args):
#    return RecordSetContainer(**resource_paths(args))
