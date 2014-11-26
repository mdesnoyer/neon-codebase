// C++ tests for the TextDetector
// 
// Copyright: 2013 Neon Labs
// Author: Mark Desnoyer (desnoyer@neon-lab.com)

#include "TextDetection.h"

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <libgen.h> 
#include <opencv2/highgui/highgui.hpp>
#include <string>

using namespace neon;
using namespace std;
using namespace cv;

class TextDetectorTest : public ::testing::Test {
public:
  TextDetectorTest() {
    boost::scoped_ptr<char> curPath(strdup(__FILE__));
    string curDir = dirname(curPath.get());
    testFileDir_ = curDir + "/test_filter_images/";
  }

protected:
  virtual void SetUp() {
  }

  // Returns the number of letters found in the image at filename
  int GetNumLetters(const string& filename) {
    string fullFilename = testFileDir_ + filename;
    Mat image = imread(fullFilename);
    if (image.empty()) {
      // Can't be fatal because we return an int
      ADD_FAILURE() << "Count not open " << fullFilename;
      return -1;
    }
    
    TextDetector::Regions letters;
    detector_.Detect(image, &letters);

    return letters.size();      
  }

  // Returns the fraction of the image identified as text
  float GetFracText(const string& filename) {
    string fullFilename = testFileDir_ + filename;
    Mat image = imread(fullFilename);
    if (image.empty()) {
      // Can't be fatal because we return an int
      ADD_FAILURE() << "Count not open " << fullFilename;
      return -1;
    }

    Mat cutImg(image, Range(0, (int)(image.rows*0.8)));
    Mat mask = detector_.Detect(cutImg, NULL);
    return static_cast<float>(countNonZero(mask)) / (mask.rows * mask.cols);
  }

  TextDetector detector_;
  string testFileDir_;
};

// TODO(mdesnoyer): Enable this test once we can handle the hard images
TEST_F(TextDetectorTest, EasyWhiteText) {
  // All of these should have a lot of text
  //EXPECT_GT(GetFracText("small_white_text.jpg"), 0.025);
  //EXPECT_GT(GetFracText("white_serif_text.jpg"), 0.025);
  EXPECT_GT(GetFracText("small_text_black_back.jpg"), 0.025);
  EXPECT_GT(GetFracText("slide.jpg"), 0.025);
  //EXPECT_GT(GetFracText("angled_text.jpg"), 0.025);
}

// TODO(mdesnoyer): Enable this test once we can handle the hard images
TEST_F(TextDetectorTest, DISABLED_EasyDarkText) {
  // All of these should have a lot of text
  EXPECT_GT(GetFracText("cnet_black_text.jpg"), 0.025);
  EXPECT_GT(GetFracText("blue_text_white_back.jpg"), 0.025);
}

TEST_F(TextDetectorTest, Logos) {
  //EXPECT_GT(GetFracText("hgtv.jpg"), 0.025);
  EXPECT_GT(GetFracText("wsj_logo.jpg"), 0.025);
}

TEST_F(TextDetectorTest, RepeatingObjects) {
  EXPECT_LT(GetFracText("fake_bricks.jpg"), 0.025);
}

// TODO(mdesnoyer): This fails because of an icon in the background
TEST_F(TextDetectorTest, DISABLED_NoText) {
  EXPECT_LT(GetFracText("spain.png"), 0.025);
}

// TODO(mdesnoyer): Enable this test once we can handle the hard images
TEST_F(TextDetectorTest, BackgroundText) {
  EXPECT_LT(GetFracText("ctv_scroller.png"), 0.025);
  EXPECT_LT(GetFracText("talking_head_ticker.jpg"), 0.025);
  EXPECT_LT(GetFracText("cbs_newsroom.jpg"), 0.025);
  //EXPECT_LT(GetFracText("title_in_background.jpg"), 0.025);
  EXPECT_LT(GetFracText("good_morning_america.jpg"), 0.025);
}

// TODO(mdesnoyer): Enable this test once we can handle the hard images
TEST_F(TextDetectorTest, TitleCards) {  
  EXPECT_GT(GetFracText("angled_title_card.jpg"), 0.025);
  EXPECT_GT(GetFracText("sim_city_title_card.jpg"), 0.025);
  EXPECT_GT(GetFracText("stack_title.jpg"), 0.025);
  EXPECT_GT(GetFracText("stack_title.jpg"), 0.025);
  EXPECT_GT(GetFracText("stack_title2.jpg"), 0.025);
  //EXPECT_GT(GetFracText("stack_title3.jpg"), 0.025);
  //EXPECT_GT(GetFracText("stack_title4.jpg"), 0.025);
  EXPECT_GT(GetFracText("splash_title.jpg"), 0.025);
}
