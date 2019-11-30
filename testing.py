
'''
Created on Nov 26, 2019

@author: ian
'''

from transaction_manager import Transaction_Manager


def main():
    tm = Transaction_Manager()
    tm.dump()
    print()
 
    with open("input.txt", "r") as f:
        for line in f:
            tm.find_cycle()
            tm.run_ready_transactions()
            print(line.strip())
            line = line.strip()
            tm.read_instruction(line)
            print()

        print("Number of Transactions:", len(tm.txns))
        print(tm.site_manager.txns_ended_list)


if __name__ == "__main__":
    main()


