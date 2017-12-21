'''
input: 
	img_s: source img H*W
	img_t: target img H*W
	mask_t: boolean matrix represent the mask of 
	H: the homography transformation matrix

output:
	img_out: the face finish warping

reference: 
	https://www.learnopencv.com/face-swap-using-opencv-c-python/
'''

from basic_packages import * 
from functions import *

def face_warping_tri(img_s,img_t,mask_s,mask_t,points_s,points_t):

	
	img_mask_s = img_s * mask_s[..., np.newaxis]
	img_mask_t = img_t * mask_t[..., np.newaxis]

	# plt.figure()
	# plt.imshow(img_mask_t)
	# plt.show()
	# plt.figure()
	# plt.imshow(img_mask_s)
	# plt.show()
	# pdb.set_trace()
	

	points_avg = np.divide(points_s + points_t,2)
	tri_obj = scipy.spatial.Delaunay(points_avg)

	'''
	Find the triangles correspond points
	'''
	tris = tri_obj.simplices
	for tri in tris:
		src_tri_pts = points_s[tri].astype(np.float32)[np.newaxis,...]
		tar_tri_pts = points_t[tri].astype(np.float32)[np.newaxis,...]
		
		r1 = cv2.boundingRect(src_tri_pts)
		r2 = cv2.boundingRect(tar_tri_pts)
		tri1Cropped = []
		tri2Cropped = []
		for i in xrange(0, 3):
			tri1Cropped.append(((src_tri_pts[0][i][0] - r1[0]),(src_tri_pts[0][i][1] - r1[1])))
			tri2Cropped.append(((tar_tri_pts[0][i][0] - r2[0]),(tar_tri_pts[0][i][1] - r2[1])))
	 
		# Apply warpImage to small rectangular patches
		img1Cropped = img_mask_s[r1[1]:r1[1] + r1[3], r1[0]:r1[0] + r1[2]]
		warpMat = cv2.getAffineTransform( np.float32(tri1Cropped), np.float32(tri2Cropped) )
		img2Cropped = cv2.warpAffine( img1Cropped, warpMat, (r2[2], r2[3]), None, flags=cv2.INTER_LINEAR, borderMode=cv2.BORDER_REFLECT_101 )
		# Get mask by filling triangle
		mask = np.zeros((r2[3], r2[2], 3), dtype = np.float32)
		cv2.fillConvexPoly(mask, np.int32(tri2Cropped), (1.0, 1.0, 1.0), 16, 0);

		# Apply mask to cropped region
		img2Cropped = img2Cropped * mask
        
		# if cannot do affine, do homography -no emotion
		temp =img_mask_t[r2[1]:r2[1]+r2[3], r2[0]:r2[0]+r2[2]]
		if (temp.shape != mask.shape):
#			sys.path.append('/home/picsl/Desktop/CIS581/CIS581Project4PartC')
#			from face_warping_homo import face_warping_homo
#			img_mask_t, img_out_t = face_warping_homo(points_s, points_t, img_s, img_t, mask_s, mask_t)
#			return img_mask_t
			return np.array([])
        
		# Copy triangular region of the rectangular patch to the output image
		img_mask_t[r2[1]:r2[1]+r2[3], r2[0]:r2[0]+r2[2]] = img_mask_t[r2[1]:r2[1]+r2[3], r2[0]:r2[0]+r2[2]] * ( (1.0, 1.0, 1.0) - mask )
        
		img_mask_t[r2[1]:r2[1]+r2[3], r2[0]:r2[0]+r2[2]] = img_mask_t[r2[1]:r2[1]+r2[3], r2[0]:r2[0]+r2[2]] + img2Cropped
				
		'''
		Visualize delaunay triangle
		'''
		# visualize_tri(img_s,img_t,src_tri_pts,tar_tri_pts)

		
	# plt.imshow(img_mask_t)
	# plt.show()
	# pdb.set_trace()
	
	return img_mask_t

