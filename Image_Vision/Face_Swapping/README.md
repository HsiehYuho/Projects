# Face_Swapping

## Must install

* Install dlib package
* Download "shape_predictor_68_face_landmarks.dat" file 
* Create a folder call "dlib" then put the "shape_predictor_68_face_landmarks.dat" file into the folder "dlib"
* Opencv

## Built With

* Opencv (Version must higher than 3.3)
* Python 2.7


## How to run the code

1. Put the source video and target video into 'input_video' folder
2. Change the runme.py 'input_path_s' and 'input_path_t' 
3. Name the output video
4. Determine the max length of the face swap frame
5. Run the following code in the runme dictory

```
python runme.py
```

6. You can find the output video appear in 'output_video'

## Ouput Video/Images

![Alt text](./ReadMe_Pics/before_color_correct.png?raw=true "Without Color Correct")
![Alt text](./ReadMe_Pics/blend.png?raw=true "After Blending")
![Alt text](./ReadMe_Pics/Comparing_old_new_mask.png?raw=true "Comparing")
![Alt text](./ReadMe_Pics/Normalize.png?raw=true "Normalize")
![Alt text](./ReadMe_Pics/Bounding_box.png?raw=true "Bounding_box")
![Alt text](./ReadMe_Pics/Warp_face.png?raw=true "Warp_face")



NOTE: The dyssynchrony is due to the output video frame number, the effect is quite good.
* [Easy](https://youtu.be/pF82J-6JAUw)
* [Medium](https://youtu.be/v1zasU9ZLUY)
* [Hard-Multiface](https://youtu.be/Tz4_QJ86cHI)
* [Hard-High-contrast](https://youtu.be/7iv6nfuyp1A) 

## Method
1. Face feature detection  
Function:  feature_detect.py  
Description:  Use dlib library to extract feature and the output shows 68 points corresponding to each facial landmarks  

2. Mask extraction  
Function: extract_mask.py   
Description: Extract mask based on the convex hull created by feature points  


3. Mask transformation  
Functions: face_warping_homo.py, face_warping_tri.py, modify_mask.py 
Description: We considered two types of face warping. One is to use homography (perspective transformation) to preserve the emotion from original image to warped image, which means that the swapped emotion emotes in the target face. The other method we implemented is to apply delaunay triangulation on each feature points and divided face based on each triangles. We further use affine transformation to warp each triangle to generate matched emotion as target images. For our final video output, we used face_warping_tri.py instead of face_warping_homo.py because we want emotions from swapped face matches with target face.

4. Face swap and blending  
Function: ImageProcessing.py  
Description: The color for each warped images has been corrected and weight distance gradient blending is used to “blend” the swap face according to previous same place facial and surrounding color  

5. (Challenge) Image normalization  
Function: equalize.py   
Description: Use histograms equalization to normalize the input frame from source image and target image  

6. (Challenge) Multi-face detection  
Function: find_multiple_face.py  
Description: Detect certain face then decide the center of face then plot a searching box for next frame  

7. (Challenge) Face-Swap mechanism change   
Function: create_video.py  
Description: Use condition statement to change the mechanism between “frame-skip” and “use previous frame face”  

8. (Challenge) Contrast adjustment mechanism change  
Function: create_video.py  
Description: Use feature detection output to determine whether frame should be normalzied  

## Reference

1. [Face_Warping_Triangulation](https://www.learnopencv.com/warp-one-triangle-to-another-using-opencv-c-python/)

2. [Face_Warping_Homography](https://www.learnopencv.com/homography-examples-using-opencv-python-c/)

3. [Dlib](http://dlib.net/face_landmark_detection.py.html)

4. [Normalization](https://docs.opencv.org/3.2.0/d5/daf/tutorial_py_histogram_equalization.html)

5. [Blending](https://github.com/MarekKowalski/FaceSwap/blob/master/FaceSwap/)

## Contributor

Yu-Ho Hsieh
Yuemeng Li
Yuanyuan Wang
