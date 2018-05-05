import sys
import cv2
import os
import Config 
import pdb
import numpy as np
import random


# xml
import xml.etree.ElementTree as ET
from xml.dom import minidom

# cnn classifier
# from CNN_classifier.load_model import predict_label

class Rect:
	def __init__(self, obj):
		x,y,w,h = obj
		self.x = x
		self.y = y
		self.w = w
		self.h = h 
	def decompose(self):
		return self.x, self.y, self.w, self.h
'''
im: the frame 
rects_xml_list: a list of output of bbox
lables: a list of label
filename: the stored filename
'''
def create_bbox_xml(im,rects_xml_list,labels,filename,save_path):
	save_path = save_path + 'Annotations/'
	if not os.path.exists(save_path):
		os.makedirs(save_path)

	# root
	anno = ET.Element('annotation')

	# size
	x,y,d = im.shape
	size = ET.SubElement(anno, 'size')
	width = ET.SubElement(size,'width')
	height = ET.SubElement(size,'height')
	depth = ET.SubElement(size,'depth')
	width.text = str(y)
	height.text = str(x)
	depth.text = str(d)

	for rect, label in zip(rects_xml_list, labels):
		# obj level
		obj = ET.SubElement(anno, 'object')
		name = ET.SubElement(obj,'name')
		name.text = str(label)
		difficult = ET.SubElement(obj,'difficult')
		difficult.text = str(0)		


		# set the bbox x,y
		bbox = ET.SubElement(obj,'bndbox')
		xmin, ymin, w, h = rect
		xmax = xmin + w
		ymax = ymin + h
		xmin_element = ET.SubElement(bbox,'xmin')
		xmax_element = ET.SubElement(bbox,'xmax')
		ymin_element = ET.SubElement(bbox,'ymin')
		ymax_element = ET.SubElement(bbox,'ymax')
		xmax_element.text = str(xmax)
		xmin_element.text = str(xmin)
		ymax_element.text = str(ymax)
		ymin_element.text = str(ymin)
	
	# create a new XML file with the results
	xmlstr = minidom.parseString(ET.tostring(anno)).toprettyxml(indent="   ")
	with open(save_path + filename +".xml", "w") as f:
		f.write(xmlstr)				
		


def openCV_findCountour(im, save_dir,frame_num,data_num ,model):
	w_upper  = Config.CONFIG['width_upper']
	w_lower  = Config.CONFIG['width_lower']
	h_upper  = Config.CONFIG['heigth_upper']
	h_lower  = Config.CONFIG['heigth_lower']
	a_upper  = Config.CONFIG['area_upper']
	a_lower  = Config.CONFIG['area_lower']
	expansion = Config.CONFIG['expansion']
	overlap = Config.CONFIG['overlap']
	deduct = Config.CONFIG['deduct']
	surround = Config.CONFIG['surround']

	test_percent = Config.CONFIG['test_percent']


	if not os.path.exists(save_dir):
		os.makedirs(save_dir)
	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

	trainval_folder = save_dir + "ImageSets/Main/"
	if not os.path.exists(trainval_folder):
		os.makedirs(trainval_folder)

	jpegImages_folder = save_dir+"JPEGImages/"
	if not os.path.exists(jpegImages_folder):
		os.makedirs(jpegImages_folder)

	
	imOut = im.copy()
	gray=cv2.cvtColor(im,cv2.COLOR_BGR2GRAY)
	y_limit, x_limit  = gray.shape
	threshold = cv2.adaptiveThreshold(gray,225,cv2.ADAPTIVE_THRESH_MEAN_C,cv2.THRESH_BINARY,surround,deduct)
	# ret,threshold = cv2.threshold(gray,0,255,cv2.THRESH_BINARY+cv2.THRESH_OTSU)
	im2, contours, hierarchy = cv2.findContours(threshold, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)


	rects_xml_list = list()	# xml list
	labels = list() # xml img label
	rect_set = list() # overlap list
	idx = 1

	for box in contours:
		x,y,w,h = cv2.boundingRect(box)
		
		# remove outsize (too large and too small) bbox 
		if(w < w_upper  and w > w_lower and h < h_upper and h > h_lower and w*h < a_upper and w*h > a_lower):
			if(y-expansion*h > 0 and y + expansion*h < y_limit):
				y = np.ceil(y - expansion*h).astype(int)
				h = np.floor((1+2*expansion)*h).astype(int)
			if(x-expansion*w > 0 and x+expansion*w < x_limit):
				x = np.ceil(x-expansion*w).astype(int)
				w = np.floor((1+2*expansion)*w).astype(int)
			
			# remove the duplicate box
			tmp_set = list()
			found = False
			for exist_r in rect_set:
				x_e, y_e, w_e, h_e = cv2.boundingRect(exist_r)
				x_middle = (x + w/2) 
				y_middle = (y + h/2 ) 
				x_e_middle = (x_e + w_e/2) 
				y_e_middle = (y_e + h_e/2)  
				if(abs(x_middle-x_e_middle) < overlap*w and abs(y_middle -y_e_middle) < overlap*h):
					found = True
					if(w > w_e or h > h_e):
						tmp_set.append(box)	
					else:
						tmp_set.append(exist_r)
				else:
					tmp_set.append(exist_r)
			if(not found):
				tmp_set.append(box)
			rect_set = tmp_set

	# open the txt for 'trainval.txt'
	trainval_file_path = trainval_folder + "trainval.txt"
	test_file_path = trainval_folder + "test.txt"

	if os.path.exists(trainval_file_path):
		append_write_train = 'a' # append if already exists
	else:
		append_write_train = 'w' # make a new file if not
	
	if os.path.exists(test_file_path):
		append_write_test = 'a' # append if already exists
	else:
		append_write_test = 'w' # make a new file if not

	trianval_f = open(trainval_file_path,append_write_train)
	test_f = open(test_file_path,append_write_test)


	for box in rect_set:
		x,y,w,h = cv2.boundingRect(box)
		# pdb.set_trace()
		y = np.ceil(y - expansion*h).astype(int)
		h = np.floor((1+2*expansion)*h).astype(int)
		x = np.ceil(x-expansion*w).astype(int)
		w = np.floor((1+2*expansion)*w).astype(int)
		# drawing
		cv2.rectangle(imOut,(x,y),(x+w,y+h),(200,0,0),2)
		roi=im[y:y+h,x:x+w]
		rects_xml_list.append((x,y,w,h))
		# labels.appned(predict_label(model,roi)) #### need to be changed 

		# Take out comment if want to write down individual roi
		# cv2.imwrite(save_dir+ str(frame_num) +'_'+str(idx) + '.jpg', roi)
		idx += 1

	# fake labels
	labels = np.ones(len(rects_xml_list),dtype=np.int)
	# labels = np.asarray(labels.dtype=np.int)

	img_name = data_num + str(frame_num)
	# test for writing xml
	create_bbox_xml(im,rects_xml_list,labels,img_name,save_dir)
	# write up overview 
	cv2.imwrite(jpegImages_folder + img_name + ".jpg", imOut)

	# write up trainval file or test file based on test percentage
	n = random.uniform(0, 1)
	if( n < test_percent):
		test_f.write(img_name +'\n')
	else:
		trianval_f.write(img_name +'\n')

	# clean up
	cv2.destroyAllWindows()
	trianval_f.close()
	test_f.close()

def inside(x_out,y_out,w_out,h_out,x_in,y_in,w_in,h_in):
	# points
	if(x_out > x_in or y_out > y_in):
		return False
	# borders
	if(w_out < w_in or h_out < h_in):
		return False
	return True


'''
# selective method to find the bbox
def selective_search(im,save_dir,frame_num):
	w_upper  = Config.CONFIG['width_upper']
	w_lower  = Config.CONFIG['width_lower']
	h_upper  = Config.CONFIG['heigth_upper']
	h_lower  = Config.CONFIG['heigth_lower']
	a_upper  = Config.CONFIG['area_upper']
	a_lower  = Config.CONFIG['area_lower']
	expansion = Config.CONFIG['expansion']

	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

    # speed-up using multithreads
	cv2.setUseOptimized(True);
	cv2.setNumThreads(4);
 
    # resize image
	newHeight = 540
	newWidth = int(im.shape[1]*newHeight/im.shape[0])
	im = cv2.resize(im, (newWidth, newHeight))   
 	y_limit, x_limit = im.shape[0],im.shape[1]
    
    # create Selective Search Segmentation Object using default parameters
	ss = cv2.ximgproc.segmentation.createSelectiveSearchSegmentation()
 
    # set input image on which we will run segmentation
	ss.setBaseImage(im)
 
	ss.switchToSelectiveSearchFast()
 	# ss.switchToSelectiveSearchQuality()
 
    # run selective search segmentation on input image
	rects_xml_list = ss.process()
     
    # number of region proposals to show
	numShowRects_xml_list = 200
	idx = 1
	condition = True
	while condition:
        # create a copy of original image
		imOut = im.copy()
		rect_set = set()
        # itereate over all the region proposals
		for i, rect in enumerate(rects_xml_list):
            # draw rectangle for region proposal till numShowRects_xml_list
			found = False
			if (i < numShowRects_xml_list):
				x, y, w, h = rect

				# Remove overlap 
				# for exist_r in rect_set:
				# 	x_e, y_e, w_e, h_e = exist_r.decompose()
				# 	if(abs(x-x_e) < 10 and abs(y-y_e) < 10 ):
				# 		found = True
				# 		break
				if(not found and w > w_lower and w < w_upper and h > h_lower and  h < h_upper and w*h > a_lower and w*h < a_upper):
					if(y-expansion*h > 0 and y + expansion*h < y_limit):
						y = np.ceil(y - expansion*h).astype(int)
						h = np.floor((1+2*expansion)*h).astype(int)
					if(x-expansion*w > 0 and x + expansion*w < x_limit):
						x = np.ceil(x-expansion*w).astype(int)
						w = np.floor((1+2*expansion)*w).astype(int)
					cv2.rectangle(imOut, (x, y), (x+w, y+h), (0, 255, 0), 1, cv2.LINE_AA)
					roi=im[y:y+h,x:x+w]
					# cv2.imwrite(save_dir + str(frame_num) +'_'+str(idx) + '.jpg', roi)	
					r = Rect(rect)
					rect_set.add(r)
					idx += 1

			else:
				condition = False
				break

		# test for writing xml
		labels = np.ones(len(rects_xml_list))

	cv2.imwrite(save_dir + str(frame_num) + "_0.jpg", imOut)
	cv2.destroyAllWindows() 
'''