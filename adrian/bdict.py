import collections

# Source: http://stackoverflow.com/a/21894086
# Multiple identical values contain their keys as lists
class bidict(dict):
    def __init__(self, *args, **kwargs):
        super(bidict, self).__init__(*args, **kwargs)
        self.inverse = {}
        for key, value in super(bidict, self).iteritems():
            self.inverse.setdefault(value,[]).append(key) 

    def __setitem__(self, key, value):
        super(bidict, self).__setitem__(key, value)
        self.inverse.setdefault(value,[]).append(key)        

    def __delitem__(self, key):
        self.inverse.setdefault(self[key],[]).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]: 
            del self.inverse[self[key]]
        super(bidict, self).__delitem__(key)
        
    # return items tuple lists
    def items(self, inverse = False):
        if not inverse:
            items = super(bidict, self).items()
        else:
            items = self.inverse.items()
        
        return items
    
    # return items iterators    
    def iteritems(self, to_list = False):
        if not to_list:
            items = super(bidict, self).iteritems()
        else:
            items = self.inverse.iteritems()
            
        return items
        
class defaultbidict(collections.defaultdict):
    """
    bidict adapted to use defaultdict and multiple values in both directions
    note: obj must be list or set
    """
    def __init__(self, obj, *args, **kwargs):
        super(defaultbidict, self).__init__(obj, *args, **kwargs)
        self.inverse = collections.defaultdict(obj)
        self._invert()
        self.obj = obj
            
    def __setitem__(self, key, value):
        itervalue, iterkey = self.obj(), self.obj()
        try:
            itervalue.append(value)
            iterkey.append(key)
        except AttributeError:
            itervalue.add(value)
            iterkey.add(key)
        super(defaultbidict, self).__setitem__(key, itervalue)
        self.inverse.setdefault(value, iterkey)

    def __delitem__(self, key):
        self.inverse.setdefault(self[key],[]).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]: 
            del self.inverse[self[key]]
        super(defaultbidict, self).__delitem__(key)
        
    def fromdictpair(self, normal, inverse):
        super(defaultbidict, self).__init__(self.obj, normal)
        self.inverse = collections.defaultdict(self.obj, inverse)
    
    def append(self, key, value):
        """
        extend iterable for default type `list' (duplicate check)
        """
        self._initpair(key, value)
        try:
            if not value in self[key]:
                self[key].append(value)
            if not key in self.inverse[value]:
                self.inverse[value].append(key)
        except AttributeError:
            raise TypeError
    
    def add(self, key, value):
        """
        extend iterable for default type `set' 
        """
        self._initpair(key, value)
        try:
            self[key].add(value)
            self.inverse[value].add(key)
        except AttributeError:
            raise TypeError
            
    def items(self, inverse = False, to_list = False):
        """
        return items tuple lists
        """
        if not inverse:
            items = super(defaultbidict, self).items()
        else:
            items = super(collections.defaultdict, self.inverse).items()
            
        if to_list and self.obj != list:
            return [(k, list(v)) for k,v in items]
        else:
            return items
    
    def iteritems(self, inverse = False, to_list = False):
        """
        return items iterators
        """
        if not inverse:
            items = super(defaultbidict, self).iteritems()
        else:
            items = super(collections.defaultdict, self.inverse).iteritems()
        
        if to_list and self.obj != list:
            for key, value in items:
                yield (key, list(value))
                
    def _invert(self):
        for key, values in super(defaultbidict, self).iteritems():
            for value in values:
                try:
                    self.inverse[value].append(key) 
                except AttributeError:
                    self.inverse[value].add(key)

    def _initpair(self, key, value):
        if not key in self:
            super(defaultbidict, self).__setitem__(key, self.obj())
        if not value in self.inverse:
            self.inverse.setdefault(value, self.obj())

# Bidirectional dictionary for nested dictionaries with a specified primary value
class nestedbidict(dict):
    def __init__(self, primary_value, *args):
        super(nestedbidict, self).__init__(*args)
        self.primary_value = primary_value
        self.inverse = {}
        self._invert()
        
    def __setitem__(self, key, value):
        super(nestedbidict, self).__setitem__(key, value)
        try:
            self.inverse.setdefault(value[self.primary_value], set()).add(key)
        except KeyError:
            print "Value dictionary does not contain specified primary value (%s)" % self.primary_value
            raise ValueError
        except TypeError:
            print "Primary value is not hashable"
            raise ValueError

    def __delitem__(self, key):
        value = self[key][self.primary_value]
        self.inverse.setdefault(value, set()).remove(key)
        if value in self.inverse and not self.inverse[value]: 
            del self.inverse[value]
        super(nestedbidict, self).__delitem__(key)
    
    def _invert(self):
        for key, value in super(nestedbidict, self).iteritems():
            self.inverse.setdefault(value[self.primary_value],set()).add(key)

    def fromdictpair(self, normal, inverse):
        super(nestedbidict, self).__init__(normal)
        self.inverse = inverse

    def absorb(self, merge_nbd):
        assert merge_nbd.primary_value == self.primary_value, \
               "Primary values not identical (self: %s, other: %s)" % \
               (merge_nbd.primary_value, self.primary_value)
        new_self = self.copy()
        new_inverse = self.inverse.copy()
        new_self.update(merge_nbd)
        new_inverse.update(merge_nbd.inverse)
        
        self.fromdictpair(new_self, new_inverse)

class nestedfullbidict:
    pass

if __name__ == '__main__':
    a = defaultbidict(set)
    print