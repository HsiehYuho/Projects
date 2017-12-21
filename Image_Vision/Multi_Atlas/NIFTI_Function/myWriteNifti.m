function myWriteNifti(filename,image,spacing)

nii = make_nii(image,spacing);
nii.hrd.dime.bitpix=16;
save_nii(nii,filename);