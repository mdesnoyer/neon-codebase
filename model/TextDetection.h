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

// Not used anymore, but may check if it still works well.
// Currently not working with opencv3
#ifndef __TEXTDETECTION2_H__
#define __TEXTDETECTION2_H__

#include <opencv2/core/core.hpp>
#include <list>
#include <memory>
#include <stdint.h>
#include <vector>

namespace neon {

class LetterInfo;

class TextDetector {

public:
  typedef std::vector<cv::Point> Region;
  typedef std::vector<std::shared_ptr<Region> > Regions;
  typedef std::shared_ptr<LetterInfo> LetterCand;
  typedef std::shared_ptr<std::list<LetterCand> > LetterCluster;

  TextDetector(float maxTextSize=0.05, float minTextSize=1e-4);
  ~TextDetector() {}

  // Detects the text in an image
  //
  // Inputs:
  // image - Image to find the text in
  //
  // Outputs:
  // letters - List of points specifying each letter that was found
  // returns - Binary image with the text as white
  cv::Mat_<uint8_t> Detect(const cv::Mat& image,
                           Regions* letters) const;

private:
  // Min and max letter size in area as a fraction of the image area
  float maxTextSize_;
  float minTextSize_;

  // Uses edge enhanced MSER to find letter candidates
  void EdgeEnhancedMSER(const cv::Mat& image,
                        const cv::Mat& color_image,
                        std::vector<LetterCand>* letters) const;

  // Filters the letter candidates based on some stats about them
  void CharacterFiltering(std::vector<LetterCand>* candidates,
                          const cv::Mat& image) const;

  // Clusters the letter candidates based on their size and stroke widths
  void ClusterTextCandidates(
    const std::vector<LetterCand>& letters,
    const cv::Mat& image,
    std::vector<LetterCluster>* clusters) const;

  // From clusters, builds a list of valid text lines
  // 
  // Clusters input will unusable after this.
  void FindTextLines(std::vector<LetterCluster>& clusters,
                     std::vector<LetterCluster>* lines) const;

  // Returns true if the text line is valid
  bool IsValidTextLine(const LetterCluster& line) const;

  cv::Mat RenderLetters(const std::vector<LetterCand>& letters,
                                    const cv::Mat& image) const;
  cv::Mat RenderLines(const std::vector<LetterCluster>& lines,
                                  const cv::Mat& image) const;
};

} // namespace

#endif // __TEXTDETECTION2_H__
