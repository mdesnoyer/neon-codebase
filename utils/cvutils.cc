#include "cvutils.h"
#include <opencv2/highgui/highgui.hpp>
#include <opencv2/imgproc/imgproc.hpp>
#include <vector>
#include <boost/scoped_ptr.hpp>

//#include "visual_utility/gnuplot_i.hpp"


using namespace cv;
using namespace std;

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
void WriteNormalizedImage(const std::string& filename, const Mat& image) {
  Mat outImage;
  normalize(image, outImage, 0, 255, NORM_MINMAX,
            CV_MAKETYPE(CV_8U, image.channels()));
  imwrite(filename, outImage);
}

void DisplayNormalizedImage(const cv::Mat& image, const char* windowName) {
  Mat outImage = image.clone();
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

inline double labfunc(double val) {
  if (val > 0.008856) {
    return std::pow(val, 1/3.);
  } else {
    return 7.787 * val + 16./116;
  }
}

Scalar bgr2lab(const Scalar& bgr) {
  Matx<double, 3, 4> A(0.357580, 0.180423, 0.412453, 0.0,
                       0.715160, 0.072169, 0.212671, 0.0, 
                       0.119193, 0.950227, 0.019334, 0.0);

  Vec<double, 3> xyz = A * bgr;
  xyz[0] /= 0.950456;
  xyz[2] /= 1.088754;
    
  double ylabval = labfunc(xyz[1]);
  Scalar lab(116 * ylabval - 16,
             500 * (labfunc(xyz[0]) - ylabval),
             200 * (labfunc(xyz[2]) - ylabval),
             0.0);
  return lab;
}

}
