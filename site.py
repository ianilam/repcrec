'''
Created on Nov 26, 2019

@author: ian
'''
from variable import Variable

class Site(object):
    '''
    classdocs
    '''


    def __init__(self, site_id):
        '''
        Constructor
        '''
        self.site_id = site_id
        self.status = "normal"
        self.data_table = dict()
        self.lock_table = dict()
        self.last_fail_timestamp = -1
        self.last_recover_timestamp = -1
        self.readable_variables = set()

        for i in range(1, 21):
            if i % 2 == 0 or (i%10) + 1 == self.site_id:
                self.data_table[i] = Variable(i)
                self.lock_table[i] = None
                self.readable_variables.add(i)


    def dump_site(self): 
        print(f'Site {self.site_id} - ', end = "")
        for k,v in self.data_table.items():
            print(f'x{k}:{v.value}, ', end="")
        print()

    
    def fail(self, ts):
        # if a site fails:
        self.status = "failed"
        
        # wipe out all the locks in the lock table
        for item_id, _ in self.lock_table.items():
            self.lock_table[item_id] = None
                    
        # set the time of the site
        self.last_fail_timestamp = ts
        
        # wipe out all the variables in readable_variables
        self.readable_variables = set()
        
        print("Site", self.site_id, "failed at timestamp", ts)

    
    def recover(self, ts):
        # recover site
        # set the time of the site 
        self.status = "normal"
        
        # if item is not replicated, add back to readable_variables
        for item_id in self.data_table.keys():
            if item_id % 2 != 0:
                self.readable_variables.add(item_id)        
        
        print("Site", self.site_id, "recovered at timestamp", ts)
        self.last_recover_timestamp = ts
        print(f'x{self.readable_variables} can be read and written to')

    
    def print_lock_table(self):
        print(f'Site {self.site_id}')
        for k,v in self.lock_table.items():
            if v != None:
                print(k, v.getInfo())
            else:
                print(k, v)
