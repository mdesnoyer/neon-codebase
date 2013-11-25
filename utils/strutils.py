'''Model utility functions.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
'''

import numpy as np

def full_object_str(obj, exclude=[]):
    '''Returns a JSON-like object string.

    For those fields in exclude, the data printed will be replaced by "..."
    '''
    field_strs = []
    for name, val in obj.__dict__.items():
        s = None
        if name in exclude:
            np_arr = np.array(val)
            if len(np_arr.shape) == 0:
                s = '%s: ...' % name
            else:
                s = '%s: <%s array>' % (name, str(np_arr.shape).replace(',','x'))
        else:
            s = '%s: %s' % (name, val)
        field_strs.append(s)
    return '<%s> {%s}' % (obj.__class__.__name__, ','.join(field_strs))
