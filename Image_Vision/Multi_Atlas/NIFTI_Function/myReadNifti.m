function [image,spacing] = myReadNifti(filename)

nii = load_nii(filename);
image = double(nii.img);
spacing = nii.hdr.dime.pixdim(2:4);