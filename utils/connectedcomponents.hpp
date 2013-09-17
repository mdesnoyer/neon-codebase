// Will be in OpenCV 2.5, but we need it now

#ifndef __OPENCV_CONNECTED_COMPONENTS__
#define __OPENCV_CONNECTED_COMPONENTS__

#include <opencv2/core/core.hpp>
#include <opencv2/imgproc/types_c.h>


namespace cv
{

enum { CC_STAT_LEFT=0, CC_STAT_TOP=1, CC_STAT_WIDTH=2, CC_STAT_HEIGHT=3, CC_STAT_AREA=4, CC_STAT_MAX = 5};

// computes the connected components labeled image of boolean image ``image``
// with 4 or 8 way connectivity - returns N, the total
// number of labels [0, N-1] where 0 represents the background label.
// ltype specifies the output label image type, an important
// consideration based on the total number of labels or
// alternatively the total number of pixels in the source image.
CV_EXPORTS_W int connectedComponents(InputArray image, OutputArray labels,
                                     int connectivity = 8, int ltype=CV_32S);
CV_EXPORTS_W int connectedComponentsWithStats(InputArray image, OutputArray labels,
                                              OutputArray stats, OutputArray centroids,
                                              int connectivity = 8, int ltype=CV_32S);

} // namespace

#endif // __OPENCV_CONNECTED_COMPONENTS__
