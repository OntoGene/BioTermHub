__author__ = 'vicawil'

from collections import defaultdict
from unified_builder import RecordSetContainer, UnifiedBuilder
from settings import dpath

def get_resources():
    resource_dict = defaultdict(bool)
    with open('resources', 'r') as resources:
        for line in resources:
            key, value = line.split('\t')
            resource_dict[key] = value.rstrip()

    return resource_dict

def ub_wrapper(*args):
    resources = get_resources()
    rsc_args = {}

    for arg in args:
        rsc_arg = resources[arg]
        if rsc_arg:
            rsc_args[arg] = dpath + rsc_arg

    rsc = RecordSetContainer(**rsc_args)
