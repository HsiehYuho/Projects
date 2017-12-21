'''
File clarification:
    import basic library such as numpy and so on
'''

import numpy as np
import matplotlib.pyplot as plt
import sys, os, math, scipy.misc, pdb, collections, random, imageio, cv2, dlib

from PIL import Image
from scipy.ndimage.filters import gaussian_filter
from scipy.spatial import ConvexHull
from scipy.spatial import Delaunay
from scipy import interpolate
from matplotlib.path import Path
from numpy.linalg import inv
from tps import from_control_points


def show_img(img):
	# img = img[:, :, ::-1]
	plt.imshow(np.uint8(img))
	plt.show()

# Reference: https://www.pyimagesearch.com/2017/04/03/facial-landmarks-dlib-opencv-python/
# face_utils functions
def shape_to_np(shape, dtype="int"):
	# initialize the list of (x, y)-coordinates
	coords = np.zeros((68, 2), dtype=dtype)
 
	# loop over the 68 facial landmarks and convert them
	# to a 2-tuple of (x, y)-coordinates
	for i in range(0, 68):
		coords[i] = (shape.part(i).x, shape.part(i).y)
 
	# return the list of (x, y)-coordinates
	return coords

'''
For Delaunay Debug
'''
def visualize_tri(img_s,img_t,src_tri_pts,tar_tri_pts):
	plt.figure()
	plt.imshow(img_s)
	plt.scatter(x=src_tri_pts[:,0],y=src_tri_pts[:,1])
	plt.figure
	plt.imshow(img_t)
	plt.scatter(x=tar_tri_pts[:,0],y=tar_tri_pts[:,1])
	plt.show()
	pdb.set_trace()

