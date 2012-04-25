#### END HEADER
# a metaclass
class Registry(type):
    # store all the types we know
    def __new__(cls, name, bases, attrs):
        # create the new type
        newtype = super(Registry, cls).__new__(cls, name, bases, attrs)
        # store it
        insert = getattr(newtype, "INSERT", False)
        print "Got new class %s with insert %s" % (name, insert)
        if insert:
          cls.registered[name] = newtype
        return newtype

    @classmethod
    def class_by_name(cls, name):
        # get a class from the registerd classes
        return cls.registered[name]

    @classmethod
    def values(cls):
      return cls.registered.values()

