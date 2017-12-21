from basic_packages import *

def find_multiple_face(img,center):
	h,w = img.shape
	center_x = center[0]
	center_y = center[1]
 	remain_bottom = h - center_x
	remain_top = center_x
	remain_left = center_y
	remain_right = w - center_y

	mask = np.zeros(img.shape)
	top_most = (center_x - remain_top * 0.5).astype(int)
	bottom_most = (center_x + remain_bottom * 0.5).astype(int)
	left_most = (center_y - remain_left * 0.25).astype(int)
	right_most = (center_y + remain_right * 0.25).astype(int)
	mask[ top_most:bottom_most ,  left_most:right_most ] = 1
	mask = mask.astype(bool)
	# show_img(img * mask)
	# pdb.set_trace()
	return img * mask