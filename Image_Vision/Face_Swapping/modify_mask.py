from basic_packages import * 
#  modify the mask according to mask after homography

def modify_mask(input_img):
	input_mask = 0.299*input_img[:,:,0] + 0.587*input_img[:,:,1] + 0.114*input_img[:,:,2]	
	output_mask = (input_mask != 0)
	return output_mask
