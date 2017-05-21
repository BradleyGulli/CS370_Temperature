from mpi4py import MPI
import csv
from collections import defaultdict
from operator import itemgetter


def isFloat(val):
        try:
                float(val)
                return True
        except:
                return False

if __name__ == "__main__":
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        n = comm.Get_size()
        data = []
        d = defaultdict(list)
        if rank == 0:
                #first we get relevent data
                with open('USC00351862.csv') as csvfile:
                        read = csv.DictReader(csvfile)
                        for row in read:
                                rowDate = row['DATE'][:4]
                                if isFloat(row['TAVG']):
                                        AVG = float(row['TAVG'])
                                        info = AVG, row['DATE'][5:]
                                        d[rowDate].append(info)
        size = len(d)
        m = size // n
        toSend = []
        comm.Barrier()
        #break the data into chunks to be sent to other processes
        for j in range(n):
                data = []
                for i in range(m):
                        data.append(d.popitem())
                toSend.append(data)
        comm.Barrier()
        data = comm.scatter(toSend, root=0)

        #process the data to find the highest temp
        highest = {}
        for element in data:
                if element[0] not in highest:
                        highest[element[0]] = element[1][0]
                for i in range(len(element[1])):
                                if highest[element[0]][0] < element[1][i][0]:
                                        highest[element[0]] = element[1][i]
        comm.Barrier()
        results = comm.gather(highest, root=0)
        if rank == 0:
                res = []
                for element in results:
                        l = len(element)
                        for i in range(l):
                                res.append(element.popitem())

                res.sort(key=itemgetter(0))
                for i in range(len(res)):
                        y = res[i][0] + '-' + res[i][1][1]
                        output = 'Date:  {}  Temp:  {}C'
                        print(output.format(y, res[i][1][0]))