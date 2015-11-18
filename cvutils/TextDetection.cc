// Text detector from 
// 
// Huizhong Chen, Sam Tsai, Georg Schroth, David
// Chen, Radek Grzeszczuk, and Bernd Girod, "Robust Text Detection in
// Natural Images with Edge-enhanced Maximally Stable Extremal
// Regions", Proc. IEEE International Conference on Image Processing,
// ICIP-11, Brussels, Belgium, September 2011.
//
// Copyright: 2013 Neon Labs
// Author: Mark Desnoyer (desnoyer@neon-lab.com)

#include "TextDetection.h"

#include <algorithm>
#include <cmath>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/connected_components.hpp>
#include <math.h>
#include <limits>
#include <opencv2/features2d/features2d.hpp>
#include <opencv2/imgproc/imgproc.hpp>

#include "cvutils-inl.h"
#include "connectedcomponents.hpp"

using boost::adjacency_list;
using namespace cv;
using cvutils::ModifyEachElement;
using namespace std;

namespace neon {

class LetterInfo {
public:
  typedef TextDetector::Region Region;

  LetterInfo(const shared_ptr<Region>& region, const Mat& color_image);

  // Getters and Setters
  int height() const { return bbox_.height; }
  int width() const { return bbox_.width; }
  int area() const { return region_->size(); }
  const Rect& bbox() const { return bbox_; }
  const shared_ptr<Region>& region() const { return region_; }
  inline Point2f centroid() const;
  float meanStrokeWidth() const;
  float stdStrokeWidth() const;
  float medianStrokeWidth() const;
  float solidity() const;
  Scalar meanColor() const { return meanColor_; }

private:
  const shared_ptr<Region> region_;
  Rect bbox_;
  Scalar meanColor_;
  
  mutable float meanStrokeWidth_;
  mutable float stdStrokeWidth_;
  mutable float medianStrokeWidth_;
  mutable Point2f centroid_;
  mutable float solidity_;  // Fraction of convex hull that is included

  void CalculateStrokeWidthStats() const;

  // Adds those neighbours of the pixel that are less than the pixel
  // value (and non-zero). Used internally by CalculateStrokeWidthStats.
  inline void AddNeighbours(int* pixel, vector<int*>& neighbors,
                            int rowLength) const;

  inline void AddNeighbourIfValid(int* neighbor,
                                  vector<int*>& neighbors,
                                  int maxVal) const;
};

TextDetector::TextDetector(float maxTextSize, float minTextSize) 
  : maxTextSize_(maxTextSize), minTextSize_(minTextSize) {}

Mat_<uint8_t> TextDetector::Detect(const Mat& image,
                                   Regions* letters) const {

  // Convert the image to uint8 greyscale
  Mat _greyImage;
  const Mat* greyImage = &image;
  if (image.channels() == 3) {
    cvtColor(image, _greyImage, CV_BGR2GRAY);
    greyImage = &_greyImage;
  }
  Mat_<uint8_t> grey8;
  normalize(*greyImage, grey8, 0, 255, NORM_MINMAX);
  Mat_<Vec3b> color8(image);

  // Do contrast enhancement
  //equalizeHist(grey8, grey8);
  //cvutils::DisplayNormalizedImage(grey8, "Contrast Enhanced");

  // Find the maximally stable regions that might be text
  vector<LetterCand> letterCandidates;
  EdgeEnhancedMSER(grey8, color8, &letterCandidates);

  //cvutils::DisplayNormalizedImage(RenderLetters(letterCandidates, grey8),
  //                                              "MSER");

  // Filter letter candidates based on stasitics of a single character
  CharacterFiltering(&letterCandidates, grey8); 

  //cvutils::DisplayNormalizedImage(RenderLetters(letterCandidates, grey8),
  //                                              "Character Filtering");

  vector<LetterCluster> clusters;
  ClusterTextCandidates(letterCandidates, grey8, &clusters);

  //cvutils::DisplayNormalizedImage(RenderLines(clusters, grey8),
  //                                            "Clustering");
            
  vector<LetterCluster> textLines;
  FindTextLines(clusters, &textLines);

  //cvutils::DisplayNormalizedImage(RenderLines(textLines, grey8),
  //                                            "Line Filtering");
  //cvutils::ShowWindowsUntilKeyPress();

  Mat_<uint8_t> mask = Mat_<uint8_t>::zeros(image.rows, image.cols);
  for (auto lineI = textLines.begin(); lineI != textLines.end(); ++lineI) {
    for (auto charI = (*lineI)->begin(); charI != (*lineI)->end(); ++charI) {
      if (letters) {
        letters->push_back((*charI)->region());
      }
      for (auto ptI = (*charI)->region()->begin();
           ptI != (*charI)->region()->end();
           ++ptI) {
        mask(*ptI) = 255;
      }
    }
  }

  return mask;
}

void TextDetector::EdgeEnhancedMSER(const cv::Mat& image,
                                    const cv::Mat& color_image,
                                    std::vector<LetterCand>* letters) const {

  // First do the normal MSER
  int imageSize = image.rows * image.cols;
  vector<Region> mserRegions;
  MSER mser(5, // Delta (find regions where the pixels are the same +- delta intensity)
            imageSize * minTextSize_,
            imageSize * maxTextSize_,
            0.10, // Max variation
            0.7); // min diversity
  mser(image, mserRegions);

  // TODO(mdesnoyer): Use edge detection to minimize blurred edges of
  // the MSER regions.

  for (auto i = mserRegions.begin(); i != mserRegions.end(); ++i) {
    letters->push_back(LetterCand(new LetterInfo(
      shared_ptr<Region>(new Region(*i)), color_image)));
  }
}


void TextDetector::CharacterFiltering(vector<LetterCand>* candidates,
                                      const Mat& image) const {
  vector<LetterCand> output;
  output.reserve(candidates->size());
  for (auto i = candidates->begin(); i != candidates->end(); ++i) {
    // Filter based on the aspect ratio
    float aspect = static_cast<float>((*i)->height()) / (*i)->width();
    if (aspect < 0.4 || aspect > 10) {
      continue;
    }

    // Text can't be too small or too big
    if ((*i)->height() < 10 || (*i)->height() > (0.4 * image.rows)) {
      continue;
    }

    // TODO(mdesnoyer): Filter if there are too many holes. This might
    // have to be done by converting to bitmap version and then doing
    // connected component analysis. We might want to convert to
    // bitmap image anyway and just do subwindowing....

    // Filter if the size of the character is way bigger than the stroke width
    float char_size = norm(Vec2f((*i)->height(), (*i)->width()));
    if (char_size > 15 * (*i)->medianStrokeWidth()) {
      continue;
    }

    // Filter based on the stroke width
    if (((*i)->stdStrokeWidth() / (*i)->meanStrokeWidth()) > 0.40) {
      continue;
    }

    output.push_back(*i);
  }

  *candidates = output;
}

void TextDetector::ClusterTextCandidates(
    const vector<LetterCand>& letters,
    const Mat& image,
    vector<LetterCluster>* clusters) const {
  CV_DbgAssert(clusters);

  // Start by building the graph of similar letters
  adjacency_list<boost::vecS, boost::vecS, boost::undirectedS> graph;
  for (uint32_t i = 0u; i != letters.size(); ++i) {
    for (uint32_t j = i + 1; j < letters.size(); ++j) {
      // Are the two letters of similar heights
      float heightAspect = ((float)letters[i]->height()) / 
        letters[j]->height();
      if (heightAspect > 2.0 || heightAspect < 0.5) {
        continue;
      }

      // Are the letters too far apart
      float dist = norm(letters[i]->centroid() - letters[j]->centroid());
      if (dist > 1.5*(letters[i]->width() + letters[j]->width())) {
        continue;
      }

      // Are the two letters a similar color
      Scalar colorDiff = letters[i]->meanColor() - letters[j]->meanColor();
      double nm = norm(colorDiff);
      if (norm(colorDiff) > 20) {
        continue;
      }

      // Are the two letters of similar stroke width
      float swAspect = letters[i]->medianStrokeWidth() / 
        letters[j]->medianStrokeWidth();
      if (swAspect > 1.5 || swAspect < 0.67) {
        continue;
      }

      add_edge(i, j, graph);
    }
  }

  // Find the connected components
  vector<int> clusterIdx(letters.size());
  int nClusters = boost::connected_components(graph, &clusterIdx[0]);
  if (nClusters == 0) {
    return;
  }
  for (int i = 0u; i < nClusters; ++i) {
    clusters->push_back(LetterCluster(new list<LetterCand>()));
  }

  // Build the output clusters
  for (uint32_t i = 0u; i < letters.size(); ++i) {
    (*clusters)[clusterIdx[i]]->push_back(letters[i]);
  }
}

void TextDetector::FindTextLines(vector<LetterCluster>& clusters,
                                 vector<LetterCluster>* lines) const {
  CV_DbgAssert(lines);

  // Variables declared out here to save reallocation in deep inner loops
  Point2f anchor;
  Point2f line;
  Point2f letterVec;

  for (auto clusterI = clusters.begin(); clusterI != clusters.end();
       ++clusterI) {
    LetterCluster curCluster = *clusterI;

    // Keep finding lines in this cluster until there aren't enough
    // letters left.
    while(curCluster->size() >= 3) {
      vector<list<LetterCand>::iterator> bestLine;
      float bestRise = 100.0;

      // For each of the letter pairs, fit a line through the centroid
      // and pick the line that intersects with the most letters.
      for (list<LetterCand>::iterator letterI = curCluster->begin();
           letterI != curCluster->end();
           ++letterI) {
        for (list<LetterCand>::iterator letterJ = std::next(letterI);
             letterJ != curCluster->end();
             ++letterJ) {
          vector<list<LetterCand>::iterator> intersected;

          anchor = (*letterI)->centroid();
          line = (*letterJ)->centroid() - anchor;
          line *= 1.0 / norm(line);
          float rise = fabs(line.y);
          if (rise > 0.71) {
            // The line can't be steeper than 45 degrees
            continue;
          }
          
          for (list<LetterCand>::iterator letterK = curCluster->begin();
               letterK != curCluster->end();
               ++letterK) {
            // If the distance of the centroid of this letter to the
            // line, is smaller than half the letter height it,
            // approximiately intersects.
            letterVec = (*letterK)->centroid() - anchor;
            if (fabs(line.cross(letterVec)) < ((*letterK)->height() / 2)) {
              intersected.push_back(letterK);
            }
          }

          if (intersected.size() > bestLine.size() ||
              (intersected.size() == bestLine.size() && rise < bestRise)) {
            bestLine = intersected;
            bestRise = rise;
          }
        }
      }

      if (bestLine.size() < 3) {
        break;
      }

      LetterCluster toPush(new std::list<LetterCand>());;
      for(auto i = bestLine.begin(); i != bestLine.end(); ++i) {
        toPush->push_back(**i);
        // Remove the letter from the current cluster
        curCluster->erase(*i);
      }

      if (!IsValidTextLine(toPush)) {
        continue;
      }

      
      lines->push_back(toPush);
    }
  }
}

bool TextDetector::IsValidTextLine(const LetterCluster& line) const {
  // Filter the candidate line if most of the letters have low solidity
  int solidityCount = 0;
  for (auto i = line->begin(); i != line->end(); ++i) {
    if ((*i)->solidity() > 0.85) {
      solidityCount++;
    }
  }
  if (solidityCount > (line->size() / 2)) {
    return false;
  }

  // TODO(mdesnoyer) filter based on template matching
  return true;
}

cv::Mat TextDetector::RenderLetters(const std::vector<LetterCand>& letters,
                                    const cv::Mat& image) const {
  Mat_<uint8_t> mask = Mat_<uint8_t>::zeros(image.rows, image.cols);

  int letterNo = 1;
  for (auto letterI = letters.begin(); letterI != letters.end(); ++letterI) {
    for (auto ptI = (*letterI)->region()->begin();
         ptI != (*letterI)->region()->end();
         ++ptI) {
      mask(*ptI) = letterNo;
    }
    letterNo++;
  }
  return mask;
}

cv::Mat TextDetector::RenderLines(const std::vector<LetterCluster>& lines,
                                    const cv::Mat& image) const {
  Mat_<uint8_t> mask = Mat_<uint8_t>::zeros(image.rows, image.cols);

  int lineNo = 1;
  for (auto lineI = lines.begin(); lineI != lines.end(); ++lineI) {
    for (auto letterI = (*lineI)->begin();
         letterI != (*lineI)->end();
         ++letterI) {
      for (auto ptI = (*letterI)->region()->begin();
           ptI != (*letterI)->region()->end();
           ++ptI) {
        mask(*ptI) = lineNo;
      }
    }
    lineNo++;
  }
  return mask;
}

LetterInfo::LetterInfo(const shared_ptr<Region>& region,
                       const Mat& color_image) 
  : region_(region), bbox_(boundingRect(*region)),
    meanColor_(),
    meanStrokeWidth_(numeric_limits<float>::quiet_NaN()),
    stdStrokeWidth_(numeric_limits<float>::quiet_NaN()),
    medianStrokeWidth_(numeric_limits<float>::quiet_NaN()),
    centroid_(0, 0),
    solidity_(-1) {
  // Calculate the mean color
  Mat_<uint8_t> mask = Mat_<uint8_t>::zeros(bbox_.height,
                                            bbox_.width);
  for(auto i = region_->begin(); i != region_->end(); ++i) {
    const Point localPt = (*i) - bbox().tl();
    mask(localPt.y, localPt.x) = 1;
    centroid_.x += i->x;
    centroid_.y += i->y;
  }
  centroid_ *= 1.0f / region_->size();
  meanColor_ = cvutils::bgr2lab((1./255) * cv::mean(color_image(bbox_), mask));
}

float LetterInfo::meanStrokeWidth() const {
  if (isnan(meanStrokeWidth_)) {
    CalculateStrokeWidthStats();
  }
  return meanStrokeWidth_;
}

float LetterInfo::stdStrokeWidth() const {
  if (isnan(stdStrokeWidth_)) {
    CalculateStrokeWidthStats();
  }
  return stdStrokeWidth_;
}

float LetterInfo::medianStrokeWidth() const {
  if (isnan(medianStrokeWidth_)) {
    CalculateStrokeWidthStats();
  }
  return medianStrokeWidth_;
}

float LetterInfo::solidity() const {
  if (solidity_ < 0) {
    vector<Point> hull;
    convexHull(*region_, hull);
    solidity_ = region_->size() / contourArea(hull);
  }
  return solidity_;
}

inline void LetterInfo::AddNeighbourIfValid(int* neighbor,
                                            vector<int*>& neighbors,
                                            int maxVal) const {
  const int val = *neighbor;
  if (val > 0 && val < maxVal) {
    neighbors.push_back(neighbor);
  }
}

inline void LetterInfo::AddNeighbours(int* pixel, vector<int*>& neighbors,
                                      int rowLength) const {
  const int val = *pixel;
  AddNeighbourIfValid(pixel-rowLength-1, neighbors, val);
  AddNeighbourIfValid(pixel-rowLength, neighbors, val);
  AddNeighbourIfValid(pixel-rowLength+1, neighbors, val);
  AddNeighbourIfValid(pixel-1, neighbors, val);
  AddNeighbourIfValid(pixel+1, neighbors, val);
  AddNeighbourIfValid(pixel+rowLength-1, neighbors, val);
  AddNeighbourIfValid(pixel+rowLength, neighbors, val);
  AddNeighbourIfValid(pixel+rowLength+1, neighbors, val);
}

void LetterInfo::CalculateStrokeWidthStats() const {
  // First build the bitmap of the letter candidate (with a black border)
  Mat_<uint8_t> bitmap = Mat_<uint8_t>::zeros(bbox_.height + 2,
                                              bbox_.width + 2);
  for(auto i = region_->begin(); i != region_->end(); ++i) {
    const Point localPt = (*i) - bbox().tl();
    bitmap(localPt.y+1, localPt.x+1) = 1;
  }

  // Calculate the stroke width for each pixel
  Mat_<float> distsf(bitmap.rows, bitmap.cols);
  distanceTransform(bitmap, distsf, CV_DIST_L2, CV_DIST_MASK_PRECISE);

  // Round the distances. Why isn't this function in opencv?!??
  Mat_<int> swt = ModifyEachElement<float(*)(float), float>(distsf,
                                                            std::round);
  int* swtPtr = swt.ptr<int>(0);
  vector<int*> neighbors;
  const int charSize = swt.cols * (swt.rows-2);
  for (int stroke = cvutils::max(swt); stroke > 0; --stroke) {
    // First add all the neighbours of those pixels with this
    // stroke. We don't need to look at the top and bottom rows.
    for (int i = swt.cols; i < charSize; ++i) {
      if (swtPtr[i] == stroke) {
        AddNeighbours(swtPtr + i, neighbors, swt.cols);
      }
    }

    // Now relabel values going downhill
    while (neighbors.size() > 0) {
      int* curPix = neighbors.back();
      neighbors.pop_back();
      AddNeighbours(curPix, neighbors, swt.cols);
      *curPix = stroke;
    }
  }


  // Now that we have the stroke width for each pixel (actually 1/2
  // the stroke width), calculate some statistics.
  int countSum = 0;
  float sum = 0;
  float sum2 = 0;
  vector<int> foreDists;
  for (int i = swt.cols; i < charSize; ++i) {
    const int val = swtPtr[i] * 2;
    if (val > 0) {
      countSum++;
      sum +=  val;
      sum2 += val * val;
      foreDists.push_back(val);
    }
  }

  CV_DbgAssert(countSum > 0);
  if (countSum == 0) {
    // In case we're in release mode
    meanStrokeWidth_ = 1;
    stdStrokeWidth_ = 0;
    medianStrokeWidth_ = 0;
  }
  meanStrokeWidth_ = sum / countSum;
  stdStrokeWidth_ = sqrt(sum2 / countSum - 
                         (meanStrokeWidth_ * meanStrokeWidth_));
  int middleIdx = foreDists.size() / 2;
  nth_element(foreDists.begin(),
              foreDists.begin() + middleIdx,
              foreDists.end());
  medianStrokeWidth_ = foreDists[middleIdx];
}

Point2f LetterInfo::centroid() const {
  return centroid_;
}

} // namespace neon
