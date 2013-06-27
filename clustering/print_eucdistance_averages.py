from scipy import *
from pylab import *
import scipy.spatial.distance as DIST
import os, glob
import sys

mtrxCL =[] 
path = os.getcwd()
signatureList = [[]]
MAXCOUNT = 250

i=0;
#SORT the list so it becomes easier
path = path + "/" + sys.argv[1] + "/" 
dirList=os.listdir(path)
dirList.sort()
for infile in  dirList: 
    if infile.endswith('.npy'):
        smarray = np.load(path+infile)
        #savefig("smarray"+str(i))    ; cla()
        mtrxCL.append(smarray)
        i = i +1
        #print i, infile

MAXCOUNT = i
mtrx = matrix(mtrxCL) 
#print var(DIST.pdist(mtrx,'euclidean',p=2))
#plot(DIST.pdist(mtrx,'euclidean',p=2))
#show()


distmt = DIST.pdist(mtrx,'euclidean',p=2)
c = 0;
newMtrx = np.zeros(shape=(MAXCOUNT,MAXCOUNT))
for i in range(MAXCOUNT):
    for j in range ( i+1,MAXCOUNT):
        newMtrx[i][j] = distmt[c]
        newMtrx[j][i] = distmt[c]
        c = c+1;
        
cla()
imshow(newMtrx,aspect='auto',interpolation='nearest', origin='lower' ) #Normalize())
x = colorbar()

#print "extremas ", x.get_clim()[1]
shape = newMtrx.shape
for i in range(shape[0]):
    print np.mean(newMtrx[i]) 

if not os.path.exists("matrix"):
    os.mkdir("matrix")

#savefig("matrix/" + sys.argv[1].split("/")[1] + ".png") 
#show()
# Comparisons of PDIST in sets     
