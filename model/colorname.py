import os.path
import sys
import numpy as np
from scipy.stats import entropy
from numpy.linalg import norm
import model.features
import glob
import urllib

__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

w2c_data = np.load(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                'w2c.dat')))
w2c_data = w2c_data[0:, 3:]
w2c_max = np.argmax(w2c_data, axis=1)
print w2c_max

import cv2

# Jensen-Shannon divergence
# https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence
def JSD(P, Q):
    P = np.array(P)
    Q = np.array(Q)
    new_P = P[P + Q > 0]
    new_Q = Q[P + Q > 0]
    if len(new_P) == 0 or len(new_Q) == 0:
        return 0
    _P = P / norm(new_P, ord=1)
    _Q = Q / norm(new_Q, ord=1)
    _M = 0.5 * (_P + _Q)
    return 0.5 * (entropy(_P, _M) + entropy(_Q, _M))

COLOR_VALUES = np.array(
                [[0., 0., 0.], [1., 0., 0.], [.25, .4, .5], [.5, .5, .5],
                [0., 1., 0.], [0., .8, 1.], [1., .5, 1.], [1., 0., 1.],
                [0., 0., 1.] , [1., 1., 1.], [0., 1., 1.]])

class ColorName(object):
    '''For a given image, returns the colorname histogram.'''    

    def __init__(self, image):
    	self.image = image

    # order of color names: black, blue, brown, grey,
    #                       green, orange, pink, purple,
    #                       red, white, yellow

    def image_to_colorname_color(self):
        self._image_to_colorname()
        self.new_color_image = (COLOR_VALUES[self.colorname_image]*255)
        self.new_color_image = np.uint8(self.new_color_image)
        cv2.imshow('win', self.new_color_image)
        # cv2.imshow('win', self.image)
        ret = cv2.waitKey()

    def _image_to_colorname(self):
        BB = self.image[0:, 0:, 0].astype(int) / 8
        GG = self.image[0:, 0:, 1].astype(int) / 8
        RR = self.image[0:, 0:, 2].astype(int) / 8
        index_im = RR + 32 * GG + 32 * 32 * BB 
        self.colorname_image = w2c_max[index_im]

    def get_colorname_histogram(self):
        self._image_to_colorname()
        hist_result = np.histogram(self.colorname_image, bins=11)[0]
        normalized_hist = hist_result.astype(float)/sum(hist_result)
        return normalized_hist

    def get_single_pixel(self, pix_val):
        index = pix_val[2]/8 + pix_val[1]/8*32 + pix_val[2]/8*32*32
        return w2c_max[index]

    @classmethod
    def get_distance(cls, image_1, image_2):
        cn_1 = cls(image_1)
        cn_2 = cls(image_2)
        hist_1 = cn_1.get_colorname_histogram()
        hist_2 = cn_2.get_colorname_histogram()
        return JSD(hist_1, hist_2)

similar_pairs = [
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d1f22b8b7b317de3c7df67d8d17f232_86cc34d6b40c77d3b3df47a6ce4e7fd2.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d1f22b8b7b317de3c7df67d8d17f232_7625999a3d87eec82268ec48d513b9f1.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d9b30b7632ad3c91ccd58ecd6f9ff1d_2d67dd65db9d98386cd362730ad0cfa0.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d9b30b7632ad3c91ccd58ecd6f9ff1d_86c1101713876f9459a12ecdbc936969.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d020c3c936b22eb456e4ceafe9ae612_1cffc3046819dc43adbc425f14fda86b.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_3d020c3c936b22eb456e4ceafe9ae612_9747552b33ae8d3177af3562b6acd48b.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_4e1ac974a11589c71032cf4ab5b84c47_99c726ee2bce0c86d00186fd587d72b3.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_4e1ac974a11589c71032cf4ab5b84c47_921742a413813d453d9580e040a91ba5.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_8cb07dd4abec525d4459139734d571d8_1d53b780e2e23cfd12133012aac20f3d.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_8cb07dd4abec525d4459139734d571d8_300ec7987c2d0d7c3c09ef8b46f9c4d6.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_9d853b1762416503f45eb9f6e072f9e4_1c114e42f1001404c395fd5ee8b71484.jpg',
     '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_9d853b1762416503f45eb9f6e072f9e4_46ea4bbada5daaab533fd1c8016b5233.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_9e11803a1131fabc0554c69d2f046702_06a8017084ba75adc3783fc328c614b4.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_9e11803a1131fabc0554c69d2f046702_76153890d8a500feba796f3a388e1155.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_13c5e73e774dbcb7059e84985ebabd5d_2d7af1d28998a3a452e12e9932e29676.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_13c5e73e774dbcb7059e84985ebabd5d_9aa899c1fb9169674a2b05c3389f8d14.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_91f96d771ab8261bc5cfbaa28bf0251d_7464474ddec079fad556eff751662e1b.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_91f96d771ab8261bc5cfbaa28bf0251d_ff44b280467982283eb57fbe42dfec08.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_826de24505722ee6b4e09eb01b3c234d_ab0997a4a093fd4ece08e53d7a259cb1.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_826de24505722ee6b4e09eb01b3c234d_bf1e0f66743522698acc80ba5add77b1.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_1470a314e580f7f313736f84a75c940d_0015f303823a8c613fafb450b0056754.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_1470a314e580f7f313736f84a75c940d_96b0b8c072c0e2b1aaace7f24e8066c0.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_7549ae3f6d7ff57192c93c30ccda6d9a_974b7865232a0a09abaa5aef0a1b1afc.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_7549ae3f6d7ff57192c93c30ccda6d9a_148cc24e2b5b04e36f08bfe6064c5c2e.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_369223f11243c86391f3d69e691354fe_98051c5c6dfca80acacdb03ea00a692e.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_369223f11243c86391f3d69e691354fe_bd32bbf649664c2d2eb962ce358332ca.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_920624bac489ffd8d55986959f9e25c6_1431df70428496b19d37a28320afeda6.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_920624bac489ffd8d55986959f9e25c6_d73f6a6705b54e89752c8ebea033a3a1.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_bccb2a89cc3763b80fb9f9e83b293576_ab9ff389a336e71e993e267a844e2ba2.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_bccb2a89cc3763b80fb9f9e83b293576_c45f6df1a56a164ca8a08269ef3ea3a0.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_c5aca0fafe6569529746e6a676f8ed6b_18874af05f4360657d80e95d23ad6d71.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_c5aca0fafe6569529746e6a676f8ed6b_fbce5e85ea3353c43f8986b27e02c23d.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_c5286f9ebab5a620bde20d24a6b33757_37061b984feb214687b7d9b8a65cb386.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_c5286f9ebab5a620bde20d24a6b33757_eb01889263a6a9a1bb7e9068d5259d14.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_ccd1ff6aa53033751f91419db795211a_a5e043ba3ac5be36618a55a38e298d20.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_ccd1ff6aa53033751f91419db795211a_f3099a56591ea257d360a63359b8dc06.jpg'),
('/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_f2d818c4df0b48c18b2a300150cfe8fc_8a9de421c973abe00091d624e80163fc.jpg',
    '/home/wiley/src/data/good_images/9xmw08l4ln1rk8uhv3txwbg1_f2d818c4df0b48c18b2a300150cfe8fc_b68bf1197e8b807bca051ff1424c5e6b.jpg')
]

def resize_image(image, max_height = 480):
    '''Resizes the image according to max_height.'''
    if ((max_height is not None) and 
        (image.shape[0] > max_height)):
        new_size = (int(2*round(float(image.shape[1]) * 
                                max_height / image.shape[0] /2)),
                                max_height)
        return cv2.resize(image, new_size)
    return image

def get_file_list(dirname = '/home/wiley/src/data/good_images/'):
    return glob.glob(dirname+"/*.jpg")

def get_random_pairs(file_name_list, num_pair = 30):
    first = np.random.random_integers(0, len(file_name_list) - 1, size=num_pair)
    second = []
    for f in first:
        s = np.random.random_integers(0, len(file_name_list) - 1)
        while np.abs(s - f) < 50:
            s = np.random.random_integers(0, len(file_name_list) - 1)
        second.append(s)
    index_pair = zip(first, second)
    return [(file_name_list[x[0]], file_name_list[x[1]]) for x in index_pair]

def get_pair_scores(file_pairs):
    gist = model.features.MemCachedFeatures.create_shared_cache(
                model.features.GistGenerator())
    gist_scores = []
    colorname_scores = []
    for pair in file_pairs:
        im_1 = cv2.imread(pair[0])
        im_2 = cv2.imread(pair[1])
        image_1 = resize_image(im_1)
        image_2 = resize_image(im_2)
        # cv2.imshow('win1', image_1)
        # cv2.imshow('win2', image_2)
        # cv2.waitKey()
        g_1 = gist.generate(image_1)
        g_2 = gist.generate(image_2)
        gist_dis = JSD(g_1, g_2)
        gist_scores.append(gist_dis)


        cn_1 = ColorName(image_1)
        cn_2 = ColorName(image_2)
        colorname_dis = ColorName.get_distance(image_1, image_2)
        colorname_scores.append(colorname_dis)
    return (gist_scores, colorname_scores)

def find_cut_out_limit(similar_values, random_values):
    center_similar = np.mean(similar_values)
    center_random = np.mean(random_values)
    min_error_count = 0
    error_count_array = []
    cut_value_array = []
    for mid in np.arange(center_similar,
                         center_random,
                         (center_random - center_similar)/100):
        similar_error = sum(np.array(similar_values) >= mid)
        random_error = sum(np.array(random_values) < mid)
        error_count_array.append(similar_error + random_error)
        cut_value_array.append(mid)
    error_count_array = np.array(error_count_array)
    cut_value_array = np.array(cut_value_array)
    min_error = np.amin(error_count_array)
    best_cut = np.mean(cut_value_array[error_count_array == min_error])    
    return (min_error, best_cut)

def image_from_url(url):
    req = urllib.urlopen(url)
    arr = np.asarray(bytearray(req.read()), dtype=np.uint8)
    img = cv2.imdecode(arr,-1) # 'load it as it is'
    return img

def get_scores_of_two_similar_images(url_1, url_2):
    gist = model.features.MemCachedFeatures.create_shared_cache(
                model.features.GistGenerator())
    image_1 = resize_image(image_from_url(url_1))
    image_2 = resize_image(image_from_url(url_2))

    g_1 = gist.generate(image_1)
    g_2 = gist.generate(image_2)
    gist_dis = JSD(g_1, g_2)

    cn_1 = ColorName(image_1)
    cn_2 = ColorName(image_2)
    colorname_dis = ColorName.get_distance(image_1, image_2)

    return (gist_dis, colorname_dis)

def main():
    similar_values = get_pair_scores(similar_pairs)
    random_pairs = get_random_pairs(get_file_list())
    random_values = get_pair_scores(random_pairs)
    print similar_values
    print random_values
    return (similar_values, random_values)


    # for pair in get_random_pairs(get_file_list()):
    #     im_1 = cv2.imread(pair[0])
    #     im_2 = cv2.imread(pair[1])
    #     image_1 = resize_image(im_1)
    #     image_2 = resize_image(im_2)
    #     cv2.imshow('win1', image_1)
    #     cv2.imshow('win2', image_2)
    #     cv2.waitKey()


    # image_1 = cv2.imread('/home/wiley/Pictures/face.jpg')
    # image_2 = cv2.imread('/home/wiley/Downloads/ColorNaming/car.jpg')
    # cn_1 = ColorName(image_1)
    # cn_2 = ColorName(image_2)
    # hist_1 = cn_1.get_colorname_histogram()
    # hist_2 = cn_2.get_colorname_histogram()
    # colorname_dis = ColorName.get_distance(image_1, image_2)

    # gist = model.features.MemCachedFeatures.create_shared_cache(
    #             model.features.GistGenerator())
    # g_1 = gist.generate(image_1)
    # g_2 = gist.generate(image_2)
    # gist_dis = JSD(g_1, g_2)
    # cn_1.image_to_colorname_color()
    # cn_2.image_to_colorname_color()

if __name__ == "__main__":
    main()
