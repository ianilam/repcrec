'''
Created on Nov 26, 2019

@author: ian
'''

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
        self.data_table = []
        self.lock_table = dict()

        for i in range(2, 21, 2):
            self.data_table.append(i)

        if self.site_id % 2 == 0:
            self.data_table.append(self.site_id-1)
            self.data_table.append(self.site_id-1+10)

    def show_data(self):
        print(self.data_table)
    
    def fail(self):
        pass
    
    def recover(self):
        pass