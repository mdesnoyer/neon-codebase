''' usage for multiple files -  for i in {1..10} ; do python create_valence_distribution.py stimuli_set$i; done '''

''' Creates a distribution from the input file values of net keep -return scores and assign valence scores to each image ''' 
import pylab
import sys
import matplotlib.pyplot as pyplot
import operator

#number of bins to create the distribution 
nbins = 7

map ={}
infile = sys.argv[1]
outfile = "vscore.txt"

#file format   
#fname <net valence score>
with open(infile) as f:
    for f in f.readlines():
        vals = f.split('\t')
        fname = vals[0]
        score = float(vals[1].rstrip('\n'))
        map[fname] = score

sorted_map = sorted(map.iteritems(), key=operator.itemgetter(1)) #tuple
x = [ v[1] for v in sorted_map ]
hist = pylab.hist(x,nbins)
bin_distribution = hist[0]

f = open(outfile,'a')
idx = 0
vscore = 1 
for b in bin_distribution:
    for i in range(b):
        f.write( sorted_map[idx][0] + " " + str(vscore) + "\n")
        idx += 1

    vscore += 1


