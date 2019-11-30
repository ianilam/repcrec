'''
Created on Nov 28, 2019

@author: ian
'''

class Variable(object):
    '''
    classdocs
    '''


    def __init__(self, var_id):
        '''
        Constructor
        '''
        self.id = var_id
        self.value = 10*var_id
