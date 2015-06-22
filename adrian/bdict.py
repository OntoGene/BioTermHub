from collections import defaultdict

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
        
# bidict adapted to use defaultdict and multiple values in both directions
class defaultbidict(defaultdict):
    def __init__(self, obj, *args, **kwargs):
        super(defaultbidict, self).__init__(obj, *args, **kwargs)
        self.inverse = defaultdict(obj)
        for key, value in super(defaultbidict, self).iteritems():
            self.inverse[value].append(key) 
        self.obj = obj
            
    def __setitem__(self, key, value):
        super(defaultbidict, self).__setitem__(key, value)
        try:
            self.inverse.setdefault(value, self.obj()).append(key)
        except TypeError:
            self.inverse.setdefault(value, self.obj()).add(key)

    def __delitem__(self, key):
        self.inverse.setdefault(self[key],[]).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]: 
            del self.inverse[self[key]]
        super(defaultbidict, self).__delitem__(key)
        
    #for default type `list' (duplicate check)
    def append(self, key, value):
        self._initpair(key, value)
        try:
            if not value in self[key]:
                self[key].append(value)
            if not key in self.inverse[value]:
                self.inverse[value].append(key)
        except AttributeError:
            raise TypeError
    
    #for default type `set'
    def add(self, key, value):
        self._initpair(key, value)
        try:
            self[key].add(value)
            self.inverse[value].add(key)
        except AttributeError:
            raise TypeError
            
    # return items tuple lists
    def items(self, inverse = False, to_list = False):
        if not inverse:
            items = super(defaultbidict, self).items()
        else:
            items = super(defaultdict, self.inverse).items()
            
        if to_list and self.obj != list:
            return [(k, list(v)) for k,v in items]
        else:
            return items
    
    # return items iterators    
    def iteritems(self, inverse = False, to_list = False):
        if not inverse:
            items = super(defaultbidict, self).iteritems()
        else:
            items = super(defaultdict, self.inverse).iteritems()
        
        if to_list and self.obj != list:
            for key, value in items:
                yield (key, list(value))

    def _initpair(self, key, value):
        if not key in self:
            super(defaultbidict, self).__setitem__(key, self.obj())
        if not value in self.inverse:
            self.inverse.setdefault(value, self.obj())
