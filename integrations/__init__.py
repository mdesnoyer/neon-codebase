from .brightcove import BrightcoveIntegration
from .cnn import CNNIntegration
from .fox import FoxIntegration
from cmsdb import neondata

def create_ovp_integration(account_id, dbobj):
    '''Factory method to create the correct subclass for a given DB object.

    dbobj - A neondata.AbstractIntegration object
    '''
    if isinstance(dbobj, neondata.BrightcoveIntegration):
        return BrightcoveIntegration(account_id, dbobj)
    elif isinstance(dbobj, neondata.CNNIntegration):
        return CNNIntegration(account_id, dbobj)
    elif isinstance(dbobj, neondata.FoxIntegration):
        return FoxIntegration(account_id, dbobj)
    raise NotImplementedError('No OVP Integration for account %s type %s'
                              % (account_id, dbobj.__class__))
