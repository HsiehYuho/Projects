import cv2 
import pdb
import numpy as np
from Functions import *
import Config 
import sys
import shutil
# from CNN_classifier.load_model import load_model

# Config
video_start = Config.CONFIG['video_start']
video_end = Config.CONFIG['video_end']

save_dir_ct = Config.CONFIG['crop_ct_dir']
# Use selective search 
# use_ss = False


if __name__ == "__main__":
	file_path = sys.argv[1]
	paths = file_path.split("/");
	data_num = os.path.splitext(paths[1])[0]

	vidcap = cv2.VideoCapture(file_path)
	[success, im] = vidcap.read()
	if(not success):
		sys.exit('fail to read video')

	dirpath = save_dir_ct+data_num+'/'

	# Clear up previous result
	if os.path.exists(dirpath):
		shutil.rmtree(dirpath)

	# Load the pretrain cnn model
	model = None
	# model = load_model()
	# Start to loop through video frame
	frame = 0
	while(vidcap.isOpened() and frame < video_end):
		[success, im] = vidcap.read()
		if(not success):
			break
		if(frame >= video_start and frame % 40 == 0):
			openCV_findCountour(im,dirpath,frame,data_num,model)
			# selective_search(im,dirpath,frame)
			print('finish: {}'.format(frame))
		frame += 1

		
	vidcap.release()


