from PIL import Image
import svmlight
import sys
import os
import glob
import leargist
import numpy
import math

#Globals


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
      print os.listdir(model_directory)
      for i,name in irange(os.listdir(model_directory)):
        if modelExt not in name:
            continue
        self.img.append( svmlight.read_model( model_directory + "/" + name))

    def create_feature_vectors(self,dir,loadFeatures = 0):
      path = os.getcwd()
      size = 256, 256
      if loadFeatures == 1 :
        for i,dirname in irange(dirList):
            for imgFile in glob.glob( os.path.join(path + "/" + dir + "/" +dirname+"/", '*.npy')):
                descriptors = numpy.load(imgFile) 
                loadArray = descriptors.tolist()
                tup_list=[]
                for k in range(len(loadArray)):
                    tmp_tuple= (k+1,loadArray[k])
                    tup_list.append(tmp_tuple)

                tup = (dirname,tup_list)
                self.training_data.append(tup)
        return

      elif loadFeatures == 0:
        for i,dirname in irange(dirList):
            image_files = glob.glob( os.path.join(path + "/" + dir + "/" +dirname+"/", '*.jpg'))
            for imgFile in image_files:
                im = Image.open(imgFile)
                im.thumbnail(size,Image.ANTIALIAS)
                descriptors = leargist.color_gist(im)
                loadArray = descriptors.tolist()
                numpy.save(imgFile + ".npy",loadArray);
                tup_list=[]
                for k in range(len(loadArray)):
                    tmp_tuple= (k+1,loadArray[k])
                    tup_list.append(tmp_tuple)

                tup = (dirname,tup_list)
                self.training_data.append(tup)
        return

    ###########################
    #Create Model Data
    ###########################
    
    def create_model(self):
      if not os.path.exists(modelDir):
        os.mkdir(modelDir)

      for i,dev in irange(dirList):
        for tuple in self.training_data:
          flag = -1
          if tuple[0] == dev:
            flag = 1
          tuple_obj = (flag,tuple[1])
          self.image_train_data[i].append(tuple_obj)

        print "creating model " + dev 
        print "items " + str(len(self.image_train_data[i]) )
        model_data = svmlight.learn(self.image_train_data[i], type='classification', verbosity=1,kernel_param=1)
        #model_data = svmlight.learn(self.image_train_data[i], type='classification', verbosity=1,kernel='polynomial',poly_degree=2)
        #model_data = svmlight.learn(self.image_train_data[i], type='ranking', verbosity=1,kernel_param=1)
        self.image_model_data.append(model_data)
        svmlight.write_model(model_data, modelDir + "/" + dev + modelExt)

    def create_model_nonbinary(self):
      if not os.path.exists(modelDir):
        os.mkdir(modelDir)

      for i,dev in irange(dirList):
        for tuple in self.training_data:
          if tuple[0] == dev:
            tuple_obj = (1,tuple[1])
            self.image_train_data[i].append(tuple_obj)


        print "creating model " + dev 
        print "items " + str(len(self.image_train_data[i]) )
        model_data = svmlight.learn(self.image_train_data[i], type='classification', verbosity=1,kernel_param=1)
        #model_data = svmlight.learn(self.image_train_data[i], type='ranking', verbosity=1,kernel_param=1)
        self.image_model_data.append(model_data)
        svmlight.write_model(model_data, modelDir + "/" + dev + modelExt)
    
    def predictions(self,image,size=(256,256)):
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


      maxScore = max(preds)
      maxIndex = preds.index(maxScore)
      #return maxIndex +1

      #Secondary ranking scheme
      preds.pop(maxIndex)
      secondMaxScore = max(preds)
      secondMaxIndex = preds.index(secondMaxScore) 

      #dummy correction for the decimal range 
      if maxIndex ==0 and secondMaxIndex ==0:
        secondMaxIndex = 1  #if already ascending order

      score = (maxIndex + 1) + (secondMaxIndex * (1.0/(len(dirList) -1)))
      res = min(len(dirList),score)
      return round(res,2) #2 floating point precision
      
    def run(self,imgFile,size=(256,256)):
        im = Image.open(imgFile)
        return self.predictions(im,size)

#MAIN
if __name__ == "__main__":
    
    # ./BinarySVM <Model directory name> 
    try:
        dir = sys.argv[1]
        binary = int(sys.argv[2])
    except:
        print "BinarySVM <Train data directory name> <binary>"
        exit(1)

    modelDir = "model_" + dir
    dirList = os.listdir(dir) #glob.glob(os.path.join(os.getcwd() + "/" + dir, '*'))
    try:
        dirList.remove('.DS_Store')
    except:
        pass

    nScoreRanges = len(dirList)

    b = BinarySVM()
    b.create_feature_vectors(dir,1)
    #b.create_feature_vectors(dir,0)

    if binary == 0:
        b.create_model_nonbinary()
    else:
        b.create_model()

    # Generate the expected list
    f = open( modelDir + "/expected.txt" ,"w")
    dirs = os.listdir(dir)
    for dirname in os.listdir(dir):
        if dirname.startswith('.'):
            continue

        for files in os.listdir(dir + "/" + dirname):
            if files.endswith(".npy"):
                f.write(files + " " + dirname + "\n")

    f.close()
