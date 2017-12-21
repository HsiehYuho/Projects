% Reslice nii image
addpath('NIFTI_Function/');

filename = './Test_Data/Images/Label_Nii/021_Label.nii';
reslice_nii(filename,'./021_Label.nii');
[a,b] = myReadNifti('./021_Label.nii');

%crop image
crop = 0;
if(crop == 1)
    cropped = crop3dimg(vox1, img1);
    figure
    myView(cropped, spacing);
    figure
    myView(img1, spacing);
end