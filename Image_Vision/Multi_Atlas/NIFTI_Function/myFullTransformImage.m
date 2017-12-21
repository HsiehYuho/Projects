function J_resliced = myFullTransformImage(p,J,aff_only,method)
%myFullTransformImage Apply affine and deformable transform to an image
%   J_resliced = myFullTransformImage(p,J)
%   J_resliced = myFullTransformImage(p,J,aff_only)
%   J_resliced = myFullTransformImage(p,J,aff_only,method)
%
%   Applies the affine and deformable transforms computed by myFullReg
%   to reslice a moving image J into the space of the fixed image. 
%
%   The inputs to this routine are:
%       p          Registration results returned by myFullReg
%       J          Image to transform (i.e., moving image or its
%                  segmentation)
%       aff_only   When equal to 1, only the affine part of the transform
%                  will be applied. Default is 0.
%       method     'linear*' for linear interpolation (default), 
%                  'nearest*' for nearest neighbor interpolation

% Handle optional parameter method
if nargin < 4
    method = 'linear*';
end

if nargin < 3
    aff_only = 0;
end


% Get the X, Y and Z coordinates of all voxels in the fixed image
sz = p.param.img.I_size;
[px,py,pz] = ndgrid(1:sz(1), 1:sz(2), 1:sz(3));

% Apply the deformable transformation first
if aff_only ~= 1
    px = px + p.deform.ux;
    py = py + p.deform.uy;
    pz = pz + p.deform.uz;
end

% Apply the affine transform
A = p.affine.A; b = p.affine.b;
qx = A(1,1) * px(:) + A(1,2) * py(:) + A(1,3) * pz(:) + b(1);
qy = A(2,1) * px(:) + A(2,2) * py(:) + A(2,3) * pz(:) + b(2);
qz = A(3,1) * px(:) + A(3,2) * py(:) + A(3,3) * pz(:) + b(3);

% Clear some memory
clear('px','py','pz');

% Interpolate the moving image at the new coordinates
samples = interpn(J, qx, qy, qz, method, 0);

% Clear some memory
clear('qx','qy','qz');

% Reorganize the samples into a volume of the same size as the fixed image
J_resliced = reshape(samples, sz);
