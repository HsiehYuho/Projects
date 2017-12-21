function F = imfilter3d(I, K)
% imfilter3d: fast filtering for 3D images
% usage:
%     F = imfilter3d(I,K)
% parameters:
%     I               3D image volume (can be any size)
%     K               3D kernel (dimensions must be odd!)
% result:
%     F               Convolution of I with K, computed using the fast
%                     Fourier transform

if ~ all(size(K) - 2 * floor(size(K)/2))
    error('The size of the kernel must be odd!');
end

% Generate the size of the fft that we will be computing
szf = size(I) + size(K) - 1;

% Compute FFT of the image
fft_I = fftn(I, szf);

% Compute FFT of the kernel
fft_K = fftn(K, szf);

% Multiply
fft_I = fft_I .* fft_K;

% Save memory
clear('fft_K');

% Compute inverse
F = real(ifftn(fft_I));

% Take the central part
shift = floor(size(K)/2);
F = F(shift(1)+1:end-shift(1),shift(2)+1:end-shift(2),shift(3)+1:end-shift(3));