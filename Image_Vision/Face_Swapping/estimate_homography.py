'''
input: 
	points_s: list of points
	points_t: list of points
	idx_s: int, the ith points in points_s list
	idx_t: int, the ith points in points_t list

output:
	H: the homography transformation transform from idx_s to idx_t

reference https://github.com/olt/thinplatespline
'''

from basic_packages import *

def estimate_homography(points_s,points_t,pix_i,pix_j,img_s,img_t):

	# idx = np.asarray([6,12,18,27])
	points_avg = np.divide(points_s + points_t,2)
	tri = Delaunay(points_avg)
	M = cv2.getAffineTransform(points_s,points_t)

	vertice_s = tri_s.simplices[num_tri]
	vertice_t = tri_t.simplices[num_tri]

	tri_pts_s = points_s[vertice_s]
	tri_pts_t = points_t[vertice_t]
	
	H_s = []
	for ptr_s,ptr_t in zip(tri_pts_s,tri_pts_t):
		H = cv2.getAffineTransform(np.float32(ptr_s), np.float32(ptr_t))
		H_s.append(H)
	# H, mask = cv2.findHomography(points_s[idx,:], points_t[idx,:], cv2.RANSAC,5.0)
	# H = est_homography(points_s[idx,0],points_s[idx,1], points_t[idx,0], points_t[idx,1])
	return H_s