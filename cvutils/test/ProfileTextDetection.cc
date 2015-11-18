#include "TextDetection.h"

#include <gflags/gflags.h>
#include <gperftools/profiler.h>
#include <opencv2/core.hpp>
#include <opencv2/highgui.hpp>
#include <iomanip>
#include <iostream>
#include <time.h>

DEFINE_string(output, "", "Output profile file");
DEFINE_int32(n, 50, "Number of times to run the text detection");

using namespace std;
using namespace cv;
using neon::TextDetector;

int mainTextDetection(int argc, char **argv ) {
  double timeSum = 0;
  TextDetector detector;
  if (!FLAGS_output.empty()) {
    ProfilerStart(FLAGS_output.c_str());
  }
  for (int i = 1; i < argc; ++i) {
    // Open the image
    Mat image = cv::imread(argv[i]);
    if (image.empty()) {
      cerr << "Could not open " << argv[i] << endl;
      continue;
    }

    clock_t startTime = clock();
    for (int j = 0; j < FLAGS_n; ++j) {
      TextDetector::Regions letters;
      detector.Detect(image, &letters);
    }
    clock_t procTime = clock() - startTime;
    timeSum += (double)(procTime) / CLOCKS_PER_SEC / FLAGS_n;
  }

  if (!FLAGS_output.empty()) {
    ProfilerStop();
  }

  cout << "Avg processing time is " 
       << setprecision(10) << timeSum  / (argc-1)
       << " seconds" << endl;

  return 0;
}

int main(int argc, char **argv ) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  string usage(argv[0]);
  google::SetUsageMessage(usage + " [options] <imageFile0> <imageFile1> ...");

  return mainTextDetection ( argc, argv );
}
