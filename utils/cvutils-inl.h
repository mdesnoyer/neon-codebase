#ifndef __CV_UTILS__INL_H__
#define __CV_UTILS__INL_H__

#include "cvutils.h"
//#include "gnuplot_i.hpp"
#include <opencv2/imgproc/imgproc.hpp>
#include <opencv2/core/core.hpp>
#include <vector>
#include <limits.h>

using std::vector;

namespace cvutils {

//Gnuplot& plotter();

// Normalize the image into the rage for ImageT
template<typename ImageT>
cv::Mat_<ImageT> NormalizeImage(const cv::Mat& image) {
  cv::Mat_<ImageT> outImage;
  double maxVal;
  switch(cv::DataType<ImageT>::depth) {
  case CV_32F:
  case CV_64F:
    maxVal = 1.0;
    break;
  case CV_8U:
  case CV_8S:
    maxVal = CHAR_MAX;
    break;
  case CV_16U:
    maxVal = USHRT_MAX;
    break;
  case CV_16S:
    maxVal = SHRT_MAX;
    break;
  case CV_32S:
    maxVal = INT_MAX;
    break;
  default:
    maxVal = 255.0;
  }
  if (!image.empty()) {
    normalize(image, outImage, 0, maxVal, cv::NORM_MINMAX,
              cv::DataType<ImageT>::depth);
  }

  return outImage;
}

// Displays a histogram of a greyscale image
//
// Inputs:
// image - The image to get the histogram of
// nBins - Number of bins in the histogram
// windowName - Name of the histogram
// redraw - Should this histogram be redrawn over the last one? If flase, a new plot is made.
// TODO(mdesnoyer): gnuplot doesn't play nice with templates so this is disabled for now.
/*template<typename ImageT>
void DisplayImageHistogram(const cv::Mat_<ImageT>& image,
                           int nBins,
                           const char* windowName,
                           bool redraw) {
  CV_Assert(image.channels() == 1);

  double maxVal;
  double minVal;
  minMaxLoc(image, &minVal, &maxVal);

  // Calculate the histogram
  vector<double> hist(nBins, 0.0);
  const double histFactor = (double)(nBins)/(maxVal - minVal);
  const double bucketSize = 1./histFactor;
  for (int row = 0; row < image.rows; ++row) {
    for (int col = 0; col < image.cols; ++col) {
      const ImageT val = image[row][col];
      if (val >= minVal && val <= maxVal) {
        hist[static_cast<int>(histFactor*(val-minVal))] += 1;
      }
    }
  }

  // Now label the centers of all the buckets
  vector<double> bucketVals;
  for (int i = 0; i < nBins; ++i) {
    bucketVals.push_back(bucketSize*(i+0.5)+minVal);
  }

  // Finally send the histogram to gnuplot
  if (redraw) {
    plotter().reset_plot();
  }
  plotter().set_style("histeps");
  plotter().plot_xy(bucketVals, hist, windowName);
  plotter().showonscreen();
  }*/


template<typename FuncT, typename ImageT>
inline void ApplyToEachElement(const cv::Mat& image, FuncT func) {
  int cols = image.cols;
  int rows = image.rows;
  if (image.isContinuous()) {
    cols *= rows;
    rows = 1;
  }
  for (int i = 0; i < rows; i++) {
    const ImageT* Mi = image.ptr<ImageT>(i);
    for(int j = 0; j < cols; j++) {
      func(Mi[j]);
    }
  }
}

template<typename FuncT, typename ImageT>
inline cv::Mat& ModifyEachElement(cv::Mat& image, FuncT func) {
  int cols = image.cols;
  int rows = image.rows;
  if (image.isContinuous()) {
    cols *= rows;
    rows = 1;
  }
  for (int i = 0; i < rows; i++) {
    ImageT* Mi = image.ptr<ImageT>(i);
    for(int j = 0; j < cols; j++) {
      Mi[j] = func(Mi[j]);
    }
  }

  return image;
}

} // namespace

#endif
