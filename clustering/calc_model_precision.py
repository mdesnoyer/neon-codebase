from PIL import Image
import svmlight
import sys
import os
import os.path
import glob
import leargist
import numpy
import math
import matplotlib.pyplot as plt
import matplotlib.cm as cm 

#Globals
train_image_folder = "trainImages"
modelDir = "model/"
modelExt = ".model"

def irange(sequence):
    return zip(range(len(sequence)), sequence)

#################################################
# Create Model
#################################################

class BinarySVM(object):
    
    def __init__(self):

      self.image_model_data = []
      self.image_train_data = []
      self.training_data = []
      self.img =[] ; # Ordering maintained in dirList

      #creat [ [] ] list
      for i in range(nScoreRanges):
        self.image_train_data.append([])
    
    def load_model(self, model_directory):
      for i,name in irange(dirList):
        if modelExt not in name:
            continue
        self.img.append( svmlight.read_model(
	    os.path.join(model_directory,name)))

    def predictions(self,image=None,size=(256,256),descriptors=None):
        if descriptors is None:
            image.thumbnail(size,Image.ANTIALIAS)
            descriptors = leargist.color_gist(image)
        
        loadArray = descriptors.tolist()
        tup_list=[]
        for k in range(len(loadArray)):
            tmp_tuple= (k+1,loadArray[k])
            tup_list.append(tmp_tuple)

        tup = (0,tup_list)
        data =[]
        data.append(tup)

        preds = []
        for i in range(nScoreRanges):
          pred = svmlight.classify(self.img[i], data)
          preds.append(pred[0])

        #print "pred scores", preds
        maxScore = max(preds)
        maxIndex = preds.index(maxScore)
        return maxIndex
    
    def run(self,imgFile,size=(256,256),descriptors=None):
        if descriptors is None:
            im = Image.open(imgFile)
            return self.predictions(im,size)
        else:
            desc = numpy.load(imgFile)
            return self.predictions(descriptors = desc)
#MAIN
if __name__ == "__main__":
    
    # ./BinarySVM <Model directory name>
    test_dir = 'testset'
    test_expected = 0
    try:
        dir = sys.argv[1]
        test_dir = sys.argv[2]
        test_expected = int(sys.argv[3])
        try: 
            prnt = int(sys.argv[4])
        except:
            prnt = 0
    except:
        print "BinarySVM <Model directory name> <test image directory> <test expected> <print/optional>"
        exit(1)

    dirList = os.listdir(dir) 
    dirList.remove('expected.txt')
    nScoreRanges = len(dirList) 

    b = BinarySVM()
    b.load_model(dir)

    correct_predictions = 0 
    predicted_scores = {}
    expected_scores = {} 
    counters = {} 
    pairs = []

    for f in open(dir + "/expected.txt").readlines():
        fname = f.split(' ')[0]
        score = f.split(' ')[1] #float(f.split(' ')[1])
        expected_scores[fname] = score.rstrip("\n") 

    for file in os.listdir(test_dir):
        if not file.endswith('.npy'):
            continue

        res = b.run(os.path.join(test_dir,file),descriptors=True)

        predicted_scores[file] = res

        pred = dirList[int(res)].split(".")   #Split the model extension

        if test_expected == 1:
            try:
                exp = expected_scores[file]
            except KeyError as e:
                continue
            predicted = pred[0]
            
            pairs.append((exp,predicted))

            if expected_scores[file] == pred[0]:
                correct_predictions +=1
            else: 
                pass
                #print file, expected_scores[file]
                #print expected_scores[file], pred[0]

        else:
            pass
            #print file, pred[0]

        try:
            val = counters[pred[0]]
            counters[pred[0]] += 1
        except:
            counters[pred[0]] = 0

    #print counters
    print "precision: " , correct_predictions *100.0 /  len(predicted_scores)

    ## Count number of training docs
    from collections import Counter
    expected_counts = Counter(expected_scores.values()).most_common()
    expc = {}
    for k,v in expected_counts:
        expc[k] = int(v)

    print expc

    ## TODO Build a confusion matrix
    mtrx = numpy.zeros(shape=(len(dirList),len(dirList)))
    for vals in pairs:
        ex = dirList.index(vals[0] + ".model")
        pred = dirList.index(vals[1] + ".model" )
        mtrx[ex][pred] +=1
        #mtrx[pred][ex] +=1
    
    '''
    for i in range(len(dirList)):
        for j in range(len(dirList)):
            val = mtrx[i][j]
            if val !=0:
                attr = dirList[i].split('.model')[0]
                mtrx[i][j] = float(val)/ expc[attr] 
    '''

    if prnt ==1:
        print dirList
        numpy.set_printoptions(linewidth=200)
        print "\n"
        print mtrx 
        
        #plt.imshow(mtrx,aspect='auto',interpolation='nearest', origin='lower' ) #Normalize())
        #plt.colorbar()
        #plt.show()
        plt.imshow(mtrx, cmap=cm.jet, interpolation='nearest')
        for i, cas in enumerate(mtrx):
            for j, c in enumerate(cas):
                if c>0:
                    plt.text(j-.2, i+.2, int(c), fontsize=10)
        plt.colorbar()
        plt.show()
