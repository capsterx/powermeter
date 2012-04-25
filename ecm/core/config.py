#### END HEADER
def cleanvalue(s):
    '''ensure that values read from configuration file are sane'''
    s = s.replace('\n', '')    # we never want newlines
    s = s.replace('\r', '')    # or carriage returns
    if s.lower() == 'false':
        s = False
    elif s.lower() == 'true':
        s = True
    return s

def pairs2dict(s):
    '''convert comma-delimited name,value pairs to a dictionary'''
    items = s.split(',')
    m = {}
    for k, v in zip(items[::2], items[1::2]):
        m[k] = v
    return m
