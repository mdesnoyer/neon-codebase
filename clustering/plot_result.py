import matplotlib.pyplot as plt
import sys
fname = sys.argv[1]
x =[]
y =[]
with open(fname,'r') as f:
    for line in f.readlines():
        vals = line.split(' ')
        x.append(int(vals[0]))
        y.append(float(vals[1]))
        
plt.plot(x,y)
plt.show()
