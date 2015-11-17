import os.path
import sys
import math
import itertools
import cv2
import numpy as np
import glob
import code
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)
from cvutils import smartcrop

def resize_and_crop(image, h, w, interpolation=cv2.INTER_AREA):
    '''Resizes the image and then crops to a new size.
    The resize preserves the aspect ratio and then the crop forces the
    image into the desired size.
    Inputs:
    image - Image to resize. Image in OpenCV format
    h - desired height
    w - desired width
    Returns: The resized and cropped image.
    '''
    scaling = max(float(h) / image.shape[0],
                  float(w) / image.shape[1])

    newsize = np.round(np.array([image.shape[0], image.shape[1]])*scaling)
    big_image = cv2.resize(image, (int(newsize[1]), int(newsize[0])),
                           interpolation=interpolation)

    sr = np.floor((newsize[0] - h)/2)
    sc = np.floor((newsize[1] - w)/2)

    return big_image[sr:sr + h, sc:sc + w, :]

def draw_faces():
    target_dir = '/home/wiley/src/data/bad_images'
    face_dst_dir = os.path.join(target_dir, 'face')
    image_files = glob.glob(os.path.join(target_dir, '*.jpg'))
    image_files.sort()
    count = 0
    smart_crop = smartcrop.SmartCrop.get_cropper()
    for im_file in image_files:
        if '_center' in im_file or '_smart' in im_file:
            continue
        count += 1
        print '(%d) %s' % (count, im_file)
        # if count == 10:
        #   break
        im = cv2.imread(im_file)
        face_im = im
        faces = smart_crop.detect_face(im)

        for box in faces:
            tl = (box[0], box[1])
            br = (box[0] + box[2], box[1] + box[3])
            cv2.rectangle(face_im, tl, br, ( 0, 255, 255 ), 3, 8)
        face_file = os.path.join(face_dst_dir, os.path.basename(im_file))
        cv2.imwrite(face_file, face_im)

def draw_saliency():
    target_dir = '/home/wiley/src/data/bad_images'
    saliency_dst_dir = os.path.join(target_dir, 'saliency')
    image_files = glob.glob(os.path.join(target_dir, '*.jpg'))
    image_files.sort()
    count = 0
    for im_file in image_files:
        if '_center' in im_file or '_smart' in im_file:
            continue
        count += 1
        print '(%d) %s' % (count, im_file)
        # if count == 10:
        #   break
        im = cv2.imread(im_file)
        saliency = smartcrop.ImageSignatureSaliency(im)
        saliency_file = os.path.join(saliency_dst_dir, os.path.basename(im_file))
        saliency_file = saliency_file.replace('.jpg', '_saliency.jpg')
        resized_file = os.path.join(saliency_dst_dir, os.path.basename(im_file))
        cv2.imwrite(saliency_file, saliency.get_saliency_map())
        cv2.imwrite(resized_file, saliency.get_resized_im())

def text():
    target_dir = '/home/wiley/src/data/bad_images'
    text_dst_dir = os.path.join(target_dir, 'text')
    image_files = glob.glob(os.path.join(target_dir, '*.jpg'))
    image_files.sort()
    count = 0
    for im_file in image_files:
        if '_center' in im_file or '_smart' in im_file:
            continue
        count += 1
        print '(%d) %s' % (count, im_file)
        # if count == 10:
        #   break
        im = cv2.imread(im_file)
        ratio = max(im.shape[0]/480.0, im.shape[1]/480.0)
        im_resized = cv2.resize(im, (int(im.shape[1]/ratio),
                                     int(im.shape[0]/ratio)))
        text_im = im_resized

        boxes, mask = cv2.text.textDetect(im_resized,
            '/home/wiley/src/opencv_contrib/modules/text/samples/trained_classifierNM1.xml',
            '/home/wiley/src/opencv_contrib/modules/text/samples/trained_classifierNM2.xml',
            16,0.00015,0.003,0.8,True,0.5, 0.9)
        for box in boxes:
            tl = (box[0], box[1])
            br = (box[0] + box[2], box[1] + box[3])
            cv2.rectangle(text_im, tl, br, ( 0, 255, 255 ), 3, 8)
        text_file = os.path.join(text_dst_dir, os.path.basename(im_file))
        cv2.imwrite(text_file, text_im)

def main():
    # draw_faces()
    # draw_saliency()
    # return

    target_dir = '/home/wiley/src/data/bad_images'
    image_files = glob.glob(os.path.join(target_dir, '*.jpg'))
    image_files.sort()
    count = 0
    for im_file in image_files:
        if '_center' in im_file or '_smart' in im_file:
            continue
        count += 1
        print '(%d) %s' % (count, im_file)
        # if count == 2:
        #   break
        im = cv2.imread(im_file)
        smart_crop = smartcrop.SmartCrop.get_cropper()
        cropped_im = smart_crop.crop(im, 600, 600)
        centered_im = resize_and_crop(im, 600, 600)
        cropped_file = im_file.replace('.jpg', '_smart.jpg')
        cv2.imwrite(cropped_file, cropped_im)
        centered_file = im_file.replace('.jpg', '_center.jpg')
        cv2.imwrite(centered_file, centered_im)

        # cv2.imshow('cropped', cropped_im)
        # cv2.imshow('centered', centered_im)
        # cv2.waitKey(0)
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
