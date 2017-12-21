from basic_packages import *

'''
    apply tps to warp the images
    https://qiita.com/SousukeShimoyama/items/2bf8defb2d057bb8b742
'''

def tps_warp(points_s, points_t, img_s, img_t, mask_s, mask_t):
    mask_s = mask_s[..., np.newaxis]
    mask_t = mask_t[..., np.newaxis]
    outface_s = img_s * mask_s
    outface_t = img_t * mask_t

    tps_s = cv2.createThinPlateSplineShapeTransformer()
    tps_t = cv2.createThinPlateSplineShapeTransformer()
    points_s =np.float32(points_s)
    points_t =np.float32(points_t)
    points_s =points_s.reshape(1, -1, 2)
    points_t =points_t.reshape(1, -1, 2)

    matches =list()

    for i in xrange(points_t.shape[1]):
        matches.append(cv2.DMatch(i, i, 0))

    tps_s.estimateTransformation(points_s, points_t, matches)
    tps_t.estimateTransformation(points_t, points_s, matches)

    img_out_s =tps_s.warpImage(outface_t, outface_s)
    img_out_t =tps_t.warpImage(outface_s, outface_t)

    plt.figure()
    plt.imshow(img_out_s)
    plt.figure()
    plt.imshow(img_out_t)
    plt.show()

    return img_out_s, img_out_t