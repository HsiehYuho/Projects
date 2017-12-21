from basic_packages import *

'''
    apply face warping 1. from source to target 2. from target to source
    this is done by using projective transformation
'''

def face_warping_homo(points_s, points_t, img_s, img_t, mask_s, mask_t):
    mask_s = mask_s[..., np.newaxis]
    mask_t = mask_t[..., np.newaxis]
    outface_s = img_s * mask_s
    outface_t = img_t * mask_t

    H_s, status_s = cv2.findHomography(points_s, points_t)
    img_out_s = cv2.warpPerspective(outface_s, H_s, (outface_t.shape[1], outface_t.shape[0]))
    H_t, status_t = cv2.findHomography(points_t, points_s)
    img_out_t = cv2.warpPerspective(outface_t, H_t, (outface_s.shape[1], outface_s.shape[0]))

    return img_out_s, img_out_t