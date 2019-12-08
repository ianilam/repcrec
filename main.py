'''
Created on Nov 26, 2019

@author: Ian Lam, Yu Ting Chiu

Advanced Database Systems Final Project

The program accepts an inputfile from the command line and starts the Transaction Manager
'''
import sys
from transaction_manager import Transaction_Manager

tm = Transaction_Manager()

# filename = sys.argv[1]

filename = "input1"


with open(filename, "r") as f:

    for line in f:
        tm.find_cycle()
        tm.run_ready_transactions()
        line = line.strip()
        tm.read_instruction(line)

    tm.find_cycle()
    tm.run_ready_transactions()

