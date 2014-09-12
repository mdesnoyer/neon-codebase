#ifndef __CV_UTILS_H__
#define __CV_UTILS_H__

#include <stdio.h>
#include <string>
#include <opencv2/core/core.hpp>

namespace cvutils {
// Writes an image to a file but first normalizes it to the 0-255 range
void WriteNormalizedImage(const std::string& filename, const cv::Mat& image);

// Displays an image to a UI window on the screen. First normalizes it
// to the 0-255 range.
void DisplayNormalizedImage(const cv::Mat& image,
                            const char* windowName="image");

// Normalize the image into the rage for ImageT
template<typename ImageT>
cv::Mat_<ImageT> NormalizeImage(const cv::Mat& image);

// Displays an LAB image on the screen converted back to RGB
void DisplayLabImage(const cv::Mat& image, const char* windowName);

// Displays a histogram of a greyscale image
//
// Inputs:
// image - The image to get the histogram of
// nBins - Number of bins in the histogram
// windowName - Name of the histogram
// redraw - Should this histogram be redrawn over the last one? If flase, a new plot is made.
template<typename ImageT> 
void DisplayImageHistogram(const cv::Mat_<ImageT>& image,
                           int nBins = 100,
                           const char* windowName="histogram",
                           bool redraw=true);

// Displays all the windows created by DisplayNormalizedImage until a
// key is pressed on the window.
void ShowWindowsUntilKeyPress();

// Calls a function for each element but cannot modify the image
//
// Types:
// FuncT - A functor type that can be called like func(ImageT element)
// ImageT - The type of the image. e.g. double
//
// Inputs:
// image - image to run through
// func - Functor to call for each element
template<typename FuncT, typename ImageT>
inline void ApplyToEachElement(const cv::Mat& image, FuncT func);

// Calls a function for each element to transform the value
//
// Types:
// FuncT - A functor type that can be called like ImageT = func(ImageT)
// ImageT - The type of the image. e.g. double
//
// Inputs:
// image - image to run through
// func - Functor to call for each element
// 
// Returns: For conviences the image that was modified
template<typename FuncT, typename ImageT>
inline cv::Mat& ModifyEachElement(cv::Mat& image, FuncT func);

template<typename FuncT, typename ImageT>
inline cv::Mat_<ImageT>& ModifyEachElement(cv::Mat_<ImageT>& image,
                                           FuncT func) {
  ModifyEachElement<FuncT, ImageT>(dynamic_cast<cv::Mat&>(image), func);
  return image;
}

// Get the min and max values of an image
double min(const cv::Mat& image);
double max(const cv::Mat& image);
double sum(const cv::Mat& image, int channel=0);

// Do scalar conversions
cv::Scalar bgr2lab(const cv::Scalar& bgr);

}

#endif // __CV_UTILS_H__
