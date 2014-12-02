/**
Python interface for the TextDetection library.

Copyright: 2013 Neon Labs
Author: Mark Desnoyer (desnoyer@neon-lab.com)
*/

#include "pycv_interface.h"
#include <opencv2/core/core.hpp>
#include <opencv2/imgproc/imgproc.hpp>
#include "TextDetection.h"

using namespace cv;

using neon::TextDetector;

//
// Creates a binary image showing the text.
// 
// Done using the stroke width transform.
// 
// From python, this should be called like:
// 
// text_image = TextDetectionPy.TextDetection(image)
//
static PyObject* pytext_textDetection(PyObject*, PyObject *args) {
  Mat cvImage;
  PyObject* image = NULL;
  if(!PyArg_ParseTuple(args, "O:TextDetection", &image)) {
    return NULL;
  }
  pycv::pyopencv_to(image, cvImage, pycv::ArgInfo("image", 0));

  TextDetector detector;
  TextDetector::Regions letters;
  Mat letterMask = detector.Detect(cvImage, NULL);

  //return Py_BuildValue("i", letters.size());
  return pycv::pyopencv_from(letterMask);
}

// List of functions exported by this module
static PyMethodDef TextDetectionPyMethods[] = {
  {"TextDetection", pytext_textDetection, METH_VARARGS,
   "Creates a binary image of detected text"},
  {NULL, NULL, 0, NULL} // Sentinel
};

// Initialize the module
PyMODINIT_FUNC initTextDetectionPy(void) {
  Py_InitModule("TextDetectionPy", TextDetectionPyMethods);
  import_array();
}

