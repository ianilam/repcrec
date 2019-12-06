'''
Created on Nov 28, 2019

@author: Ian Lam, Yu Ting Chiu
'''


class Variable(object):
    '''
    Variable is the data item.
    
    Attributes:
        id (int): id of the Variable
        value (int): value of the Variable. It's initialized as 10 * id
    '''
    def __init__(self, var_id):
        '''
        Constructor
        
        Parameters:
            var_id (int): ID of variable.
        '''
        self.id = var_id
        self.value = 10 * var_id
