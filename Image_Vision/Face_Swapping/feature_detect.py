from basic_packages import * 

def feature_detect(img):

	detector = dlib.get_frontal_face_detector()
	predictor = dlib.shape_predictor('./dlib/shape_predictor_68_face_landmarks.dat')
	rects = detector(img, 1)
	points_list = []
	for (i, rect) in enumerate(rects):
		# Shape: the points of face feature
		shape = predictor(img, rect)
		points = shape_to_np(shape)
		points_list.append(points)
		
	# # Visualize the shape
	# 	for j in range(26):
	# 		(x, y) = points[j]
	# 		cv2.circle(img, (x, y), 1, (255, 255, 0), -1)
	# cv2.imshow("Output", img)
	return points_list


