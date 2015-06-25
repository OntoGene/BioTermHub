__author__ = 'vicawil'

import unittest
import bdict
import collections

# Note: Currently, this unit test only covers absorption
# TODO: Test full functionality

class Nestedbidict(unittest.TestCase):
    def test_absorption(self):
        k, l, m, u, v, w = 0, 0, 3, 4, 7, 100
        indices = [k, l, m, u, v, w]
        main_init_dict, indices = self.dict_builder(indices, 10)
        merge_init_dict, _ = self.dict_builder(indices, 6)

        main_nbd = bdict.nestedbidict("primary", main_init_dict)
        merge_nbd = bdict.nestedbidict("primary", merge_init_dict)

        main_nbd_inverse = main_nbd.inverse
        merge_nbd_inverse = merge_nbd.inverse

        main_nbd.absorb(merge_nbd)

        goal_normal_raw = {0: {0: 4, 1: 5, 2: 6, 'primary': 100}, 1: {1: 5, 2: 6, 3: 7, 'primary': 101},
                       2: {2: 6, 3: 7, 4: 8, 'primary': 101}, 3: {3: 7, 4: 8, 5: 9, 'primary': 102},
                       4: {4: 8, 5: 9, 6: 10, 'primary': 102}, 5: {'primary': 103, 5: 9, 6: 10, 7: 11},
                       6: {8: 12, 'primary': 103, 6: 10, 7: 11}, 7: {8: 12, 9: 13, 'primary': 104, 7: 11},
                       8: {8: 12, 9: 13, 10: 14, 'primary': 104}, 9: {9: 13, 10: 14, 11: 15, 'primary': 105},
                       10: {10: 14, 11: 15, 12: 16, 'primary': 105}, 11: {11: 15, 12: 16, 13: 17, 'primary': 106},
                       12: {12: 16, 13: 17, 14: 18, 'primary': 106}, 13: {'primary': 107, 13: 17, 14: 18, 15: 19},
                       14: {16: 20, 'primary': 107, 14: 18, 15: 19}, 15: {16: 20, 17: 21, 'primary': 108, 15: 19}}
        _
        goal_inverse_raw = {100: set([0]), 101: set([1, 2]), 102: set([3, 4]), 103: set([5, 6]), 104: set([8, 7]),
                        105: set([9])}

        goal_normal = collections.OrderedDict((k, collections.OrderedDict(sorted(v.items(), key = lambda x: x[0]))) for k, v in goal_normal_raw.items())
        main_nbd_ordered = collections.OrderedDict((k, collections.OrderedDict(sorted(v.items(), key = lambda x: x[0]))) for k, v in main_nbd.items())
        goal_inverse = collections.OrderedDict(goal_inverse_raw)
        main_nbd_inverse_ordered = collections.OrderedDict(main_nbd_inverse)

        self.assertEqual(main_nbd_ordered, goal_normal)
        self.assertEqual(main_nbd_inverse_ordered, goal_inverse)

    def dict_builder(self, indices, range_idx):
        outer_dict = {}
        k, l, m, u, v, w = indices
        loop_indices = indices[1:]
        for i in range(0 + k, range_idx + k):
            nested_dict = dict(zip(range(l,m), range(u,v)))
            nested_dict["primary"] = w
            outer_dict[i] = nested_dict
            loop_indices = [x + 1 for x in loop_indices[:-1]]
            if not i % 2:
                loop_indices.append(w + 1)
            else:
                loop_indices.append(w)
            l, m, u, v, w = loop_indices
        indices = [k + range_idx] + loop_indices
        return outer_dict, indices

if __name__ == '__main__':
    unittest.main()
