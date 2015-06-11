class OID(object):
    _counter = 0
    _oid = None
    
    @classmethod
    def get(cls):
        cls._oid = OID.base36encode(OID._counter)
        cls._counter += 1
        return cls._oid
        
    @classmethod
    def last(cls):
        return cls._oid
    
    @staticmethod
    def base36encode(number, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
        """Converts an integer to a base36 string."""
        if not isinstance(number, (int, long)):
            raise TypeError('number must be an integer')
    
        base36 = ''
        sign = ''
    
        if number < 0:
            sign = '-'
            number = -number
    
        if 0 <= number < len(alphabet):
            return sign + alphabet[number]
    
        while number != 0:
            number, i = divmod(number, len(alphabet))
            base36 = alphabet[i] + base36
    
        return sign + base36
    
    @staticmethod
    def base36decode(number):
        return int(number, 36)
