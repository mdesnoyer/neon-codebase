
//
// This file is auto-generated, please don't edit!
//

#define LOG_TAG "org.opencv.contrib"

#include "common.h"
#include "opencv2/contrib/contrib.hpp"

using namespace cv;

extern "C" {


//
//  void applyColorMap(Mat src, Mat& dst, int colormap)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_Contrib_applyColorMap_10 (JNIEnv*, jclass, jlong, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_Contrib_applyColorMap_10
  (JNIEnv* env, jclass , jlong src_nativeObj, jlong dst_nativeObj, jint colormap)
{
    try {
        LOGD("contrib::applyColorMap_10()");
        Mat& src = *((Mat*)src_nativeObj);
        Mat& dst = *((Mat*)dst_nativeObj);
        cv::applyColorMap( src, dst, (int)colormap );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::applyColorMap_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::applyColorMap_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::applyColorMap_10()}");
        return;
    }
}



//
//  int chamerMatching(Mat img, Mat templ, vector_vector_Point& results, vector_float& cost, double templScale = 1, int maxMatches = 20, double minMatchDistance = 1.0, int padX = 3, int padY = 3, int scales = 5, double minScale = 0.6, double maxScale = 1.6, double orientationWeight = 0.5, double truncate = 20)
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_Contrib_chamerMatching_10 (JNIEnv*, jclass, jlong, jlong, jlong, jlong, jdouble, jint, jdouble, jint, jint, jint, jdouble, jdouble, jdouble, jdouble);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_Contrib_chamerMatching_10
  (JNIEnv* env, jclass , jlong img_nativeObj, jlong templ_nativeObj, jlong results_mat_nativeObj, jlong cost_mat_nativeObj, jdouble templScale, jint maxMatches, jdouble minMatchDistance, jint padX, jint padY, jint scales, jdouble minScale, jdouble maxScale, jdouble orientationWeight, jdouble truncate)
{
    try {
        LOGD("contrib::chamerMatching_10()");
        vector< vector<Point> > results;
        Mat& results_mat = *((Mat*)results_mat_nativeObj);
        vector<float> cost;
        Mat& cost_mat = *((Mat*)cost_mat_nativeObj);
        Mat& img = *((Mat*)img_nativeObj);
        Mat& templ = *((Mat*)templ_nativeObj);
        int _retval_ = cv::chamerMatching( img, templ, results, cost, (double)templScale, (int)maxMatches, (double)minMatchDistance, (int)padX, (int)padY, (int)scales, (double)minScale, (double)maxScale, (double)orientationWeight, (double)truncate );
        vector_vector_Point_to_Mat( results, results_mat );  vector_float_to_Mat( cost, cost_mat );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::chamerMatching_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::chamerMatching_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::chamerMatching_10()}");
        return 0;
    }
}



JNIEXPORT jint JNICALL Java_org_opencv_contrib_Contrib_chamerMatching_11 (JNIEnv*, jclass, jlong, jlong, jlong, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_Contrib_chamerMatching_11
  (JNIEnv* env, jclass , jlong img_nativeObj, jlong templ_nativeObj, jlong results_mat_nativeObj, jlong cost_mat_nativeObj)
{
    try {
        LOGD("contrib::chamerMatching_11()");
        vector< vector<Point> > results;
        Mat& results_mat = *((Mat*)results_mat_nativeObj);
        vector<float> cost;
        Mat& cost_mat = *((Mat*)cost_mat_nativeObj);
        Mat& img = *((Mat*)img_nativeObj);
        Mat& templ = *((Mat*)templ_nativeObj);
        int _retval_ = cv::chamerMatching( img, templ, results, cost );
        vector_vector_Point_to_Mat( results, results_mat );  vector_float_to_Mat( cost, cost_mat );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::chamerMatching_11() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::chamerMatching_11() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::chamerMatching_11()}");
        return 0;
    }
}



//
//  void FaceRecognizer::load(string filename)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_load_10 (JNIEnv*, jclass, jlong, jstring);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_load_10
  (JNIEnv* env, jclass , jlong self, jstring filename)
{
    try {
        LOGD("contrib::load_10()");
        FaceRecognizer* me = (FaceRecognizer*) self; //TODO: check for NULL
        const char* utf_filename = env->GetStringUTFChars(filename, 0); std::string n_filename( utf_filename ? utf_filename : "" ); env->ReleaseStringUTFChars(filename, utf_filename);
        me->load( n_filename );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::load_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::load_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::load_10()}");
        return;
    }
}



//
//  void FaceRecognizer::predict(Mat src, int& label, double& confidence)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_predict_10 (JNIEnv*, jclass, jlong, jlong, jdoubleArray, jdoubleArray);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_predict_10
  (JNIEnv* env, jclass , jlong self, jlong src_nativeObj, jdoubleArray label_out, jdoubleArray confidence_out)
{
    try {
        LOGD("contrib::predict_10()");
        FaceRecognizer* me = (FaceRecognizer*) self; //TODO: check for NULL
        Mat& src = *((Mat*)src_nativeObj);
        int label;
        double confidence;
        me->predict( src, label, confidence );
        jdouble tmp_label[1] = {label}; env->SetDoubleArrayRegion(label_out, 0, 1, tmp_label);  jdouble tmp_confidence[1] = {confidence}; env->SetDoubleArrayRegion(confidence_out, 0, 1, tmp_confidence);
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::predict_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::predict_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::predict_10()}");
        return;
    }
}



//
//  void FaceRecognizer::save(string filename)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_save_10 (JNIEnv*, jclass, jlong, jstring);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_save_10
  (JNIEnv* env, jclass , jlong self, jstring filename)
{
    try {
        LOGD("contrib::save_10()");
        FaceRecognizer* me = (FaceRecognizer*) self; //TODO: check for NULL
        const char* utf_filename = env->GetStringUTFChars(filename, 0); std::string n_filename( utf_filename ? utf_filename : "" ); env->ReleaseStringUTFChars(filename, utf_filename);
        me->save( n_filename );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::save_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::save_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::save_10()}");
        return;
    }
}



//
//  void FaceRecognizer::train(vector_Mat src, Mat labels)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_train_10 (JNIEnv*, jclass, jlong, jlong, jlong);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_train_10
  (JNIEnv* env, jclass , jlong self, jlong src_mat_nativeObj, jlong labels_nativeObj)
{
    try {
        LOGD("contrib::train_10()");
        vector<Mat> src;
        Mat& src_mat = *((Mat*)src_mat_nativeObj);
        Mat_to_vector_Mat( src_mat, src );
        FaceRecognizer* me = (FaceRecognizer*) self; //TODO: check for NULL
        Mat& labels = *((Mat*)labels_nativeObj);
        me->train( src, labels );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::train_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::train_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::train_10()}");
        return;
    }
}



//
//  void FaceRecognizer::update(vector_Mat src, Mat labels)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_update_10 (JNIEnv*, jclass, jlong, jlong, jlong);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_update_10
  (JNIEnv* env, jclass , jlong self, jlong src_mat_nativeObj, jlong labels_nativeObj)
{
    try {
        LOGD("contrib::update_10()");
        vector<Mat> src;
        Mat& src_mat = *((Mat*)src_mat_nativeObj);
        Mat_to_vector_Mat( src_mat, src );
        FaceRecognizer* me = (FaceRecognizer*) self; //TODO: check for NULL
        Mat& labels = *((Mat*)labels_nativeObj);
        me->update( src, labels );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::update_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::update_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::update_10()}");
        return;
    }
}



//
//  native support for java finalize()
//  static void FaceRecognizer::delete( __int64 self )
//
JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_delete(JNIEnv*, jclass, jlong);

JNIEXPORT void JNICALL Java_org_opencv_contrib_FaceRecognizer_delete
  (JNIEnv*, jclass, jlong self)
{
    delete (FaceRecognizer*) self;
}


//
//   StereoVar::StereoVar()
//

JNIEXPORT jlong JNICALL Java_org_opencv_contrib_StereoVar_StereoVar_10 (JNIEnv*, jclass);

JNIEXPORT jlong JNICALL Java_org_opencv_contrib_StereoVar_StereoVar_10
  (JNIEnv* env, jclass )
{
    try {
        LOGD("contrib::StereoVar_10()");
        
        StereoVar* _retval_ = new StereoVar(  );
        return (jlong) _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::StereoVar_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::StereoVar_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::StereoVar_10()}");
        return 0;
    }
}



//
//   StereoVar::StereoVar(int levels, double pyrScale, int nIt, int minDisp, int maxDisp, int poly_n, double poly_sigma, float fi, float lambda, int penalization, int cycle, int flags)
//

JNIEXPORT jlong JNICALL Java_org_opencv_contrib_StereoVar_StereoVar_11 (JNIEnv*, jclass, jint, jdouble, jint, jint, jint, jint, jdouble, jfloat, jfloat, jint, jint, jint);

JNIEXPORT jlong JNICALL Java_org_opencv_contrib_StereoVar_StereoVar_11
  (JNIEnv* env, jclass , jint levels, jdouble pyrScale, jint nIt, jint minDisp, jint maxDisp, jint poly_n, jdouble poly_sigma, jfloat fi, jfloat lambda, jint penalization, jint cycle, jint flags)
{
    try {
        LOGD("contrib::StereoVar_11()");
        
        StereoVar* _retval_ = new StereoVar( (int)levels, (double)pyrScale, (int)nIt, (int)minDisp, (int)maxDisp, (int)poly_n, (double)poly_sigma, (float)fi, (float)lambda, (int)penalization, (int)cycle, (int)flags );
        return (jlong) _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::StereoVar_11() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::StereoVar_11() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::StereoVar_11()}");
        return 0;
    }
}



//
//  void StereoVar::operator ()(Mat left, Mat right, Mat& disp)
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_compute_10 (JNIEnv*, jclass, jlong, jlong, jlong, jlong);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_compute_10
  (JNIEnv* env, jclass , jlong self, jlong left_nativeObj, jlong right_nativeObj, jlong disp_nativeObj)
{
    try {
        LOGD("contrib::compute_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        Mat& left = *((Mat*)left_nativeObj);
        Mat& right = *((Mat*)right_nativeObj);
        Mat& disp = *((Mat*)disp_nativeObj);
        me->operator ()( left, right, disp );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::compute_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::compute_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::compute_10()}");
        return;
    }
}



//
// int StereoVar::levels
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1levels_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1levels_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1levels_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->levels;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1levels_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1levels_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1levels_10()}");
        return 0;
    }
}



//
// void StereoVar::levels
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1levels_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1levels_10
  (JNIEnv* env, jclass , jlong self, jint levels)
{
    try {
        LOGD("contrib::set_1levels_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->levels = ( (int)levels );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1levels_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1levels_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1levels_10()}");
        return;
    }
}



//
// double StereoVar::pyrScale
//

JNIEXPORT jdouble JNICALL Java_org_opencv_contrib_StereoVar_get_1pyrScale_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jdouble JNICALL Java_org_opencv_contrib_StereoVar_get_1pyrScale_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1pyrScale_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        double _retval_ = me->pyrScale;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1pyrScale_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1pyrScale_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1pyrScale_10()}");
        return 0;
    }
}



//
// void StereoVar::pyrScale
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1pyrScale_10 (JNIEnv*, jclass, jlong, jdouble);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1pyrScale_10
  (JNIEnv* env, jclass , jlong self, jdouble pyrScale)
{
    try {
        LOGD("contrib::set_1pyrScale_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->pyrScale = ( (double)pyrScale );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1pyrScale_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1pyrScale_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1pyrScale_10()}");
        return;
    }
}



//
// int StereoVar::nIt
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1nIt_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1nIt_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1nIt_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->nIt;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1nIt_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1nIt_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1nIt_10()}");
        return 0;
    }
}



//
// void StereoVar::nIt
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1nIt_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1nIt_10
  (JNIEnv* env, jclass , jlong self, jint nIt)
{
    try {
        LOGD("contrib::set_1nIt_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->nIt = ( (int)nIt );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1nIt_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1nIt_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1nIt_10()}");
        return;
    }
}



//
// int StereoVar::minDisp
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1minDisp_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1minDisp_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1minDisp_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->minDisp;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1minDisp_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1minDisp_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1minDisp_10()}");
        return 0;
    }
}



//
// void StereoVar::minDisp
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1minDisp_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1minDisp_10
  (JNIEnv* env, jclass , jlong self, jint minDisp)
{
    try {
        LOGD("contrib::set_1minDisp_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->minDisp = ( (int)minDisp );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1minDisp_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1minDisp_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1minDisp_10()}");
        return;
    }
}



//
// int StereoVar::maxDisp
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1maxDisp_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1maxDisp_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1maxDisp_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->maxDisp;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1maxDisp_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1maxDisp_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1maxDisp_10()}");
        return 0;
    }
}



//
// void StereoVar::maxDisp
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1maxDisp_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1maxDisp_10
  (JNIEnv* env, jclass , jlong self, jint maxDisp)
{
    try {
        LOGD("contrib::set_1maxDisp_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->maxDisp = ( (int)maxDisp );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1maxDisp_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1maxDisp_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1maxDisp_10()}");
        return;
    }
}



//
// int StereoVar::poly_n
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1poly_1n_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1poly_1n_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1poly_1n_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->poly_n;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1poly_1n_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1poly_1n_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1poly_1n_10()}");
        return 0;
    }
}



//
// void StereoVar::poly_n
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1poly_1n_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1poly_1n_10
  (JNIEnv* env, jclass , jlong self, jint poly_n)
{
    try {
        LOGD("contrib::set_1poly_1n_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->poly_n = ( (int)poly_n );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1poly_1n_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1poly_1n_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1poly_1n_10()}");
        return;
    }
}



//
// double StereoVar::poly_sigma
//

JNIEXPORT jdouble JNICALL Java_org_opencv_contrib_StereoVar_get_1poly_1sigma_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jdouble JNICALL Java_org_opencv_contrib_StereoVar_get_1poly_1sigma_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1poly_1sigma_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        double _retval_ = me->poly_sigma;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1poly_1sigma_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1poly_1sigma_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1poly_1sigma_10()}");
        return 0;
    }
}



//
// void StereoVar::poly_sigma
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1poly_1sigma_10 (JNIEnv*, jclass, jlong, jdouble);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1poly_1sigma_10
  (JNIEnv* env, jclass , jlong self, jdouble poly_sigma)
{
    try {
        LOGD("contrib::set_1poly_1sigma_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->poly_sigma = ( (double)poly_sigma );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1poly_1sigma_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1poly_1sigma_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1poly_1sigma_10()}");
        return;
    }
}



//
// float StereoVar::fi
//

JNIEXPORT jfloat JNICALL Java_org_opencv_contrib_StereoVar_get_1fi_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jfloat JNICALL Java_org_opencv_contrib_StereoVar_get_1fi_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1fi_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        float _retval_ = me->fi;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1fi_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1fi_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1fi_10()}");
        return 0;
    }
}



//
// void StereoVar::fi
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1fi_10 (JNIEnv*, jclass, jlong, jfloat);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1fi_10
  (JNIEnv* env, jclass , jlong self, jfloat fi)
{
    try {
        LOGD("contrib::set_1fi_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->fi = ( (float)fi );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1fi_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1fi_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1fi_10()}");
        return;
    }
}



//
// float StereoVar::lambda
//

JNIEXPORT jfloat JNICALL Java_org_opencv_contrib_StereoVar_get_1lambda_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jfloat JNICALL Java_org_opencv_contrib_StereoVar_get_1lambda_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1lambda_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        float _retval_ = me->lambda;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1lambda_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1lambda_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1lambda_10()}");
        return 0;
    }
}



//
// void StereoVar::lambda
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1lambda_10 (JNIEnv*, jclass, jlong, jfloat);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1lambda_10
  (JNIEnv* env, jclass , jlong self, jfloat lambda)
{
    try {
        LOGD("contrib::set_1lambda_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->lambda = ( (float)lambda );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1lambda_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1lambda_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1lambda_10()}");
        return;
    }
}



//
// int StereoVar::penalization
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1penalization_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1penalization_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1penalization_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->penalization;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1penalization_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1penalization_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1penalization_10()}");
        return 0;
    }
}



//
// void StereoVar::penalization
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1penalization_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1penalization_10
  (JNIEnv* env, jclass , jlong self, jint penalization)
{
    try {
        LOGD("contrib::set_1penalization_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->penalization = ( (int)penalization );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1penalization_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1penalization_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1penalization_10()}");
        return;
    }
}



//
// int StereoVar::cycle
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1cycle_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1cycle_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1cycle_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->cycle;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1cycle_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1cycle_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1cycle_10()}");
        return 0;
    }
}



//
// void StereoVar::cycle
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1cycle_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1cycle_10
  (JNIEnv* env, jclass , jlong self, jint cycle)
{
    try {
        LOGD("contrib::set_1cycle_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->cycle = ( (int)cycle );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1cycle_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1cycle_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1cycle_10()}");
        return;
    }
}



//
// int StereoVar::flags
//

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1flags_10 (JNIEnv*, jclass, jlong);

JNIEXPORT jint JNICALL Java_org_opencv_contrib_StereoVar_get_1flags_10
  (JNIEnv* env, jclass , jlong self)
{
    try {
        LOGD("contrib::get_1flags_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        int _retval_ = me->flags;//(  );
        return _retval_;
    } catch(cv::Exception e) {
        LOGD("contrib::get_1flags_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return 0;
    } catch (...) {
        LOGD("contrib::get_1flags_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::get_1flags_10()}");
        return 0;
    }
}



//
// void StereoVar::flags
//

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1flags_10 (JNIEnv*, jclass, jlong, jint);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_set_1flags_10
  (JNIEnv* env, jclass , jlong self, jint flags)
{
    try {
        LOGD("contrib::set_1flags_10()");
        StereoVar* me = (StereoVar*) self; //TODO: check for NULL
        me->flags = ( (int)flags );
        return;
    } catch(cv::Exception e) {
        LOGD("contrib::set_1flags_10() catched cv::Exception: %s", e.what());
        jclass je = env->FindClass("org/opencv/core/CvException");
        if(!je) je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, e.what());
        return;
    } catch (...) {
        LOGD("contrib::set_1flags_10() catched unknown exception (...)");
        jclass je = env->FindClass("java/lang/Exception");
        env->ThrowNew(je, "Unknown exception in JNI code {contrib::set_1flags_10()}");
        return;
    }
}



//
//  native support for java finalize()
//  static void StereoVar::delete( __int64 self )
//
JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_delete(JNIEnv*, jclass, jlong);

JNIEXPORT void JNICALL Java_org_opencv_contrib_StereoVar_delete
  (JNIEnv*, jclass, jlong self)
{
    delete (StereoVar*) self;
}


} // extern "C"
