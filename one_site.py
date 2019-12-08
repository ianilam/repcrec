'''
Created on Nov 26, 2019

@author: Ian Lam, Yu Ting Chiu
'''

from variable import Variable


class Site(object):
    """
    Site is where the data items are stored. It receives directions from the site_manager regarding failing and recovering of a site. 
    
        Attributes:
        site_id (int): ID of site
        status (string): Status of the site - "normal" or "failed"
        data_table (dict (int : Variable Object) ): Item ID: Variable  Object
        lock_table (dict (int: Lock Object) ): Item ID: Lock holding that object
        last_fail_timestamp (int): time of last site failure
        last_recover_timestamp (int): time of last site recover
        readable_items (set (int) ): Item IDs of all the data items that can be read from this site at the current time

    """
    def __init__(self, site_id):
        '''
        Constructor
        
        Parameters:
            site_id (int): ID of site
        '''

        self.site_id = site_id
        self.status = "normal"
        self.data_table = dict()
        self.lock_table = dict()
        self.last_fail_timestamp = -1
        self.last_recover_timestamp = -1
        self.readable_variables = set()

        for i in range(1, 21):
            if i % 2 == 0 or (i % 10) + 1 == self.site_id:
                self.data_table[i] = Variable(i)
                self.lock_table[i] = None
                self.readable_variables.add(i)

    def dump_site(self):
        # dump info of this site
        print(f'Site {self.site_id} - ', end="")
        s = ""
        for k, v in self.data_table.items():
            # print(f'x{k}: {v.value}, ', end="")
            s += f'x{k}: {v.value}, '
        print(s[:-2])

    def fail(self, ts):
        """Fails a site, wipes out the lock table, and wipes out readable_variables
    
        Parameters:
            ts (int): time of site failure
        """

        self.status = "failed"

        # wipe out all the locks in the lock table
        for item_id, _ in self.lock_table.items():
            self.lock_table[item_id] = None

        # set the time of the site failure
        self.last_fail_timestamp = ts

        # wipe out all the variables in readable_variables
        self.readable_variables = set()

    def recover(self, ts):
        """Recovers a site, and puts back unreplicated items into the readable_variables. Replicated items need to wait for committed write to happen.
    
        Parameters:
            ts (int): time of site recovery
        """

        # recover site
        self.status = "normal"

        # if item is not replicated, add back to readable_variables
        for item_id in self.data_table.keys():
            if item_id % 2 != 0:
                self.readable_variables.add(item_id)

        # set the timestamp of the site recovery
        self.last_recover_timestamp = ts
