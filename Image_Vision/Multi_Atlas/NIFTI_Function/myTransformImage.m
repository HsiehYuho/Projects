function resampled = myTransformImage(fixed, moving, A, b, method)

% Handle optional parameter
if nargin < 5
    method = 'linear*';
end

% Get the X, Y and Z coordinates of all voxels in the fixed image
[px,py,pz] = ndgrid(1:size(fixed,1), 1:size(fixed,2), 1:size(fixed,3));

% Apply the affine transform
qx = A(1,1) * px(:) + A(1,2) * py(:) + A(1,3) * pz(:) + b(1);
qy = A(2,1) * px(:) + A(2,2) * py(:) + A(2,3) * pz(:) + b(2);
qz = A(3,1) * px(:) + A(3,2) * py(:) + A(3,3) * pz(:) + b(3);

% Clear some memory
clear('px','py','pz');

% Interpolate the moving image at the new coordinates
samples = interpn(moving, qx, qy, qz, method, 0);

% Clear some memory
clear('qx','qy','qz');

% Reorganize the samples into a volume of the same size as the fixed image
resampled = reshape(samples, size(fixed));





