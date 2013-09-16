#include "cvutils.h"
#include <opencv2/highgui/highgui.hpp>
#include <opencv2/imgproc/imgproc.hpp>
#include <vector>
#include <boost/scoped_ptr.hpp>

//#include "visual_utility/gnuplot_i.hpp"


using namespace cv;

namespace cvutils {

// TODO(mdesnoyer): gnuplot doesn't play nice with templates so this
// is disabled for now.
/* boost::scoped_ptr<Gnuplot> plotter_;

Gnuplot& plotter() {
  if (plotter_.get() == NULL) {
    plotter_.reset(new Gnuplot());
  }
  return *plotter_;
  }*/

// Writes an image to a file but first normalizes it to the 0-255 range
void WriteNormalizedImage(const string filename, const Mat& image) {
  Mat outImage;
  normalize(image, outImage, 0, 255, NORM_MINMAX,
            CV_MAKETYPE(CV_8U, image.channels()));
  imwrite(filename, outImage);
}

void DisplayNormalizedImage(const cv::Mat& image, const char* windowName) {
  Mat outImage = image;
  if (image.channels() == 1) {
    normalize(image, outImage, 0, 255, NORM_MINMAX,
              CV_MAKETYPE(CV_8U, image.channels()));
  }

  
  cvNamedWindow(windowName, CV_WINDOW_AUTOSIZE);

  IplImage showImage = outImage;
  cvShowImage(windowName, &showImage);
}

void DisplayLabImage(const cv::Mat& image, const char* windowName) {
  Mat outImage;
  Mat outImage32;
  image.convertTo(outImage32, CV_32FC3);
  cvtColor(outImage32, outImage, CV_Lab2BGR);
  
  cvNamedWindow(windowName, CV_WINDOW_AUTOSIZE);

  IplImage showImage = outImage;
  cvShowImage(windowName, &showImage);
}

void ShowWindowsUntilKeyPress() {
  int key = -1;
  while (key < 0) {
    key = cvWaitKey(100);
  }
}

double min(const Mat& image) {
  double minVal;
  minMaxLoc(image, &minVal, NULL, NULL, NULL);
  return minVal;
}

double max(const Mat& image) {
  double maxVal;
  minMaxLoc(image, NULL, &maxVal, NULL, NULL);
  return maxVal;
}

double sum(const Mat& image, int channel) {
  return cv::sum(image)[channel];
}

}
