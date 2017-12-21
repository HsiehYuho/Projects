# BECIS537-2017-RVOT

Working toward automatic RVOT segmentation for modeling of TOF

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.


### Prerequisites
* Matlab (development version 2017a)
* c3d command line tool (Must be able to use c3d from terminal)
* Atlas_Data put in Image Folder with such folders  
"Affine_Deform_Nii" (empty), "Affine_Nii" (empty), "Img_Nii", "Label_Nii", "Seg_Resample_Nii"

### Installing c3d command line tool (MAC)

1. Please go to the link then download [CONVERT3D](http://www.itksnap.org/pmwiki/pmwiki.php?n=Downloads.C3D).

2. Install the application and (remember to drag the icon to Application)

3. Open Terminal.
```
cd to ~ (default home) directory
```

4. Using the command to open/create a file call ".bash_profile.
```
vim ~/.bash_profile
```

5. Adding this line into the file (Press "I" means enter into insert mode, and after finish the editing, press "esc" to leave the insert mode, then press ":" then "wq" to leave vim mode ).
```
export PATH=/Applications/Convert3DGUI.app/Contents/bin:$PATH
```

7. Execute the command in the terminal to refresh the setting.
```
source ~/.bash_profile
```

8. Now suppose you can run c3d in the command line, test c3d -h in the terminal, it should appear something like this ...

```
Image Input/Output and Information: 
    -dicom-series-list              : List image series in a DICOM directory
    -dicom-series-read              : Read a DICOM image series
    -info                           : Display brief image information        
    -info-full                      : Display verbose image information    
```


## How to label landmark as multi-atlas 

If you have manual segmentation from 3D-Slicer, please reslice it before label landmarks steps
```
c3d -interpolation NearestNeighbor -mcs YOUR_MANUAL_SEGMENTATION.seg.nrrd -pop -popas SEG YOUR_ROVT_IMAGE.nrrd -push SEG -reslice-identity -o YOUR_RESLICE_IAMGE.nii.gz
```


###  How to label landmarks in ITK-SNAP
1. Open the RVOT image in ITK_SNAP as main image (File -> Open Main Image)
2. Load the manual segmentation as segmentation (Segmentation -> Open Segmentation)
3. Click update to see the 3d model (Update bottom is at the left bottom part)
4. Unload the manual segmentation (Segmentation -> Unload Segmentation)
5. Add the manual segmentation as an overlay image (File -> Add Another Image)
6. At "How should the image be displayed", select "as a seperate image"
7. At scroll down bottom, find "display as overlap"
8. At same scroll down bottom, find "Overlays", adjust the opacity level to the appropiate level
9. Using paintbrush model to label the landmark, please change the label number(color) every time using different color
10. After finish labeling landmarks, export segmentation as nii file

### Landmark Rules (ORDER MATTERS!!)
You must label the landmark according to the following rules. (Suggest by Dr.Jolly)
Boundaries (help crop image):
* Label 1: +X
* Label 2: -X
* Label 3: +Y
* Label 4: -Y
* Label 5: +Z
* Label 6: -Z

Registration:
* Label 7: TV (tricuspid valve)
* Label 8: RV Apex (right ventricular apex)
* Label 9: RV to MPA transition
* Label 10: branch point for main PA
* Label 11: RPA
* Label 12: LPA



## Running the tests 

### How to put your input test RVOT image? 
You must have these two types of images in "Test" folder.
1. Raw RVOT images named "Test_Img.nii".
2. Landmark segmentation using 12 landmarks named "Test_Label.nii".
3. Manual Segmentation for Raw RVOT images named "Test_Seg.nii". 
(3 is Optional, if not provide, must modify turn the variable "turn_on_validate" = 0)

### Configuration
You can determine how many atals you want to use for this registration.
```
atlas_min = 1;
atlas_max = 4;
```
If set atlas_min = 1 and atlas_max = 4, the dsc_val will output an array contains 4 numbers, which represnets the dsc score for register 1 atals to target, register 2 atlas to target, 3 ... and 4 resecptively. 

If you want to save or delete intermediate images, you can turn on or turn off control variable.
```
clear_all_intermediate_output = 0;
```

If you do not provide manaual segmenation as ground truth, please turn off the validation.
```
turn_on_validate = 0;
```


### Run the program
Open runme.m file and run the program by click run.

### Expected output
1. An output predicted segmentation in the "Output" folder.
2. An output DSC score in matlab terminal console.


## Introduction of file structure 

### Folders
1. "Helper_Function" contains the necessary functions to run automatic RVOT segmentation.
2. "Input" contains the test images, label, segmentation(optional)
3. "NIFTI_Function" contains the third party functions provided by Jimmy Shen, 2009
4. "Output" contains the predict segmentation
5. "Result" contains the temporary results which worth discuss
7. "Test_Data" contains all the atlas images in sub folder "Images"

### The runme.m file
The main structure of runme.file includes three parts.
1. Read the atals images, segmentations and landmarks_label file.
2. Transform landmarks to points and calculate the accordingly H (affine transformation) matrix.
3. Apply H matrix on corresponding images and do the deform registration to get Ux, Uy, Uz deform vector.
4. Apply H matrix and Ux, Uy, Uz to segmentation.
5. Apply majority voting algorithm to get the predicted segmentation. 
6. (Optional) Validate the predict segmentation with the manual segmentation(ground truth). 

## Results


## Built With
*  Mac OS 10.13

## Authors
Yu-Ho Hsieh 2017

## Contributor
Chloe Snyder, Robert Gorman

### Reference
Copyright (c) 2009, Jimmy Shen


