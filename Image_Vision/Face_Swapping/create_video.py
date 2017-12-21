from basic_packages import * 
from functions import *
'''
input: 
	file_path_s: the path of the mp4 file of source
	file_path_t: the path of the mp4 file of target
	len_frame: int
	file_output_path: the path of output video
'''

def create_video(file_path_s, file_path_t ,len_frame,output_path_s,output_path_t):

	''' 
	Create video capture object
	'''
	vidcap_s = cv2.VideoCapture(file_path_s)
	vidcap_t = cv2.VideoCapture(file_path_t)
	
	'''
	use first frame to create the video screen size 
	'''
	[success_s, img_s] = vidcap_s.read()
	[success_t, img_t] = vidcap_t.read()
	if(not success_s or not success_t):
		sys.exit('fail to read video ')

	'''
 	init video object
	'''
	h_s , w_s , l_s =  img_s.shape
	fourcc = cv2.VideoWriter_fourcc(*'MJPG')
	video_s = cv2.VideoWriter(output_path_s,fourcc,20,(w_s,h_s),True)

	h_t , w_t , l_t =  img_t.shape
	fourcc = cv2.VideoWriter_fourcc(*'MJPG')
	video_t = cv2.VideoWriter(output_path_t,fourcc,20,(w_t,h_t),True)

	'''
	config
	'''
	visual_corresponding_point = False
	count = 0
	multiple_face = False
	keep_emotion = False

	'''
	default center
	'''
	center = np.asarray([h_t/2,w_t/2])


	'''
	store previous images
	'''
	first_detect_s = False
	first_detect_t = False
	pre_img_s = None
	pre_img_t = None




	while (vidcap_s.isOpened() and vidcap_t.isOpened() and count < len_frame):
		[success_s, img_s] = vidcap_s.read(count)
		[success_t, img_t] = vidcap_t.read(count)

		if(not success_s or not success_t):
			break;


		# if(count < 3):
		# 	count += 1
		# 	continue
		img_s_gray = cv2.cvtColor(img_s, cv2.COLOR_BGR2GRAY)
		img_t_gray = cv2.cvtColor(img_t, cv2.COLOR_BGR2GRAY)

		'''
			Normalize the image
		'''
		img_s_gray = equalize(img_s_gray)
		img_t_gray = equalize(img_t_gray)

		'''
			BGR to RGB
		'''
		# img_s = img_s[:, :, ::-1]
		# img_t = img_t[:, :, ::-1]

		points_list_s = feature_detect(img_s_gray)

		if(multiple_face):
			img_t_gray = find_multiple_face(img_t_gray,center)
			points_list_t = feature_detect(img_t_gray)
		else:
			points_list_t = feature_detect(img_t_gray)


		'''
			Update previous image according whether the image can be detected
			if face in A image can be detected, we update pre_img_A
		'''

		if(len(points_list_s) != 0 and len(points_list_t) != 0):
			first_detect_s = True
			first_detect_t = True
			pre_img_s = img_s_gray
			pre_img_t = img_t_gray

		elif(len(points_list_s) != 0 and len(points_list_t) == 0 and (first_detect_t)):
			first_detect_s = True
			pre_img_s = img_s_gray
			points_list_t = feature_detect(pre_img_t)

		elif(len(points_list_s) == 0 and len(points_list_t) != 0 and (first_detect_s)):
			first_detect_t = True			
			pre_img_t = img_t_gray
			points_list_s = feature_detect(pre_img_s)

		else:
			video_s.write(np.uint8(img_s))
			video_t.write(np.uint8(img_t))
			count += 1
			continue


		'''
		Visualize the shape
		'''

		if(visual_corresponding_point):
			points_s = points_list_s[0]
			points_t = points_list_t[0]

			for j in range(68):
				(x, y) = points_s[j]
				cv2.circle(img_s, (x, y), 1, (255, 255, 0), -1)
			plt.figure()
			cv2.imshow("Output_1", img_s)
		
			for j in range(68):
				(x, y) = points_t[j]
				cv2.circle(img_t, (x, y), 1, (255, 255, 0), -1)
			plt.figure()
			cv2.imshow("Output_2", img_t)
			pdb.set_trace()



		masks_s = extract_mask(points_list_s,img_s_gray)
		masks_t = extract_mask(points_list_t,img_t_gray)

		mask_s = masks_s[0]
		mask_t = masks_t[0]


		points_s = points_list_s[0].astype(np.float)
		points_t = points_list_t[0].astype(np.float)
		
		'''
		Re-detect middle point and swap the axes
		'''
		new_center_y, new_center_x = np.average(points_t,axis=0)
		center = np.asarray([new_center_x,new_center_y])

		'''
			apply face warping 1. from source to target 2. from target to source
		'''
		if(keep_emotion):
			img_out_t = face_warping_tri(img_s,img_t,mask_s,mask_t,points_s,points_t)
			img_out_s = face_warping_tri(img_t,img_s,mask_t,mask_s,points_t,points_s)

		else:
			img_out_s, img_out_t = face_warping_homo(points_s, points_t, img_s, img_t, mask_s, mask_t)

		'''
			Modify mask according to the face warping results
		'''
		mask_s_new = modify_mask(img_out_t)
		mask_t_new = modify_mask(img_out_s)


		'''
			this is for image seamlessclone
			img_out_s with outbody_t
			img_out_t with outbody_s
		'''
		img_out_s = ImageProcessing.colorTransfer(img_t, img_out_s, mask_t_new)
		blend_t  = ImageProcessing.blendImages(img_out_s, img_t, mask_t_new)
         
		img_out_t = ImageProcessing.colorTransfer(img_s, img_out_t, mask_s_new)
		blend_s  = ImageProcessing.blendImages(img_out_t, img_s, mask_s_new)

		video_s.write(np.uint8(blend_s))
		video_t.write(np.uint8(blend_t))
		count += 1
		print(count)


	cv2.destroyAllWindows()
	video_s.release()
	video_t.release()
	vidcap_s.release()
	vidcap_t.release()
	print('finish swapping')
