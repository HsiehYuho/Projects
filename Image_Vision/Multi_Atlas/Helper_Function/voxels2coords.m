function coords = voxels2coords(fnimg,voxels)

% -------------------------------------------------------------------------
% This function converts landmarks in a given image from voxels to physical
% coordinates. 
%
% INPUT:
%
%   fnimg -- filename of the image that is landmarked
%   voxels -- Nx3 matrix of landmarks in voxel coordinates, where N is the
%             number of 3D landmarks
%
% OUTPUT:
%
%   coords -- Nx3 matrix of landmarks in physical coordinates, where N is
%   the number of 3D landmarks
%
% -------------------------------------------------------------------------

% check if the image file exists
if ~exist(fnimg,'file')
    error([fnimg ' does not exist.']);
end

% read the nifti image
nii = read_img_zip(fnimg);

% voxel to coordinate transform transform
tform = [nii.hdr.hist.srow_x; 
            nii.hdr.hist.srow_y; 
            nii.hdr.hist.srow_z; 
            0 0 0 1];

% voxel indices in homogeneous form
v = [voxels'; ones(1,size(voxels,1))];

% get physical coordinates
c = tform*v;
coords = c(1:3,:)';
