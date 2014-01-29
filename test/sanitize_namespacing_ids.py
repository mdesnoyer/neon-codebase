''' Sanitize account ids be removing any prefixes, Add api key mappings '''

import os.path
import sys
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] <> base_path:
    sys.path.insert(0,base_path)

from supportServices import neondata
import utils

def main():
    
    #Get all user accounts
    nuser_accounts = neondata.NeonUserAccount.get_all_accounts()
    db_connection = neondata.DBConnection("NeonApiKey")
    #For a given user get all integrations, update account ids
    for nuser in nuser_accounts:
        a_id = nuser.account_id.split('_')[-1]
        key = neondata.NeonApiKey.format_key(a_id)
        new_integrations = {}
        db_connection.blocking_conn.set(key, nuser.neon_api_key)
        nuser.account_id = a_id
        platforms = nuser.get_platforms() 
        for platform in platforms:
            #remove any prefixes
            platform.account_id = a_id 
            platform.integration_id = platform.integration_id.split('_')[-1]
            #recreate key
            platform.key = platform.__class__.__name__.lower() +\
                    '_%s_%s' %(platform.neon_api_key, platform.integration_id)
            print a_id, platform.integration_id
            platform.save() 
        
        #update integration ids
        for key, value in nuser.integrations.iteritems():
            k = key.split('_')[-1]
            new_integrations[k] = value
        nuser.integrations = new_integrations
        nuser.save()

if __name__ == "__main__":
    utils.neon.InitNeon()
    main()
