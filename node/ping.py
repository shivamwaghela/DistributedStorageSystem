import time
import os
import sys
print(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.dirname(os.path.realpath(__file__)))


def ping():
    file = open("/Users/shivam/PycharmProjects/DistributedStorageSystem/node/connection_list.txt", "r")
    connection_list = file.readlines()
    file.close()

    if len(connection_list) == 0:
        return

    tries = 5
    i = 0
    while True:
        if i == len(connection_list):
            return

        ip = connection_list[i]
        response = os.system("ping -c 5 " + ip)
        print(response)

        if response == 0:
            print(ip, 'is up!')
            i += 1
            tries = 5
        else:
            print(ip, 'is down!')
            tries -= 1

        time.sleep(5)


if __name__ == "__main__":
    ping()
