function filtered = myGaussianLPF(image, sigma)

% Compute the radius of the kernel
radius = ceil(3 * sigma);

% Generate the 1D Gaussian
t = -radius:radius;
Gt = exp(-0.5 * t.^2 / sigma.^2);

% Take the outer product
[Gx,Gy,Gz] = ndgrid(Gt,Gt,Gt);
K = Gx .* Gy .* Gz;

% Normalize the kernel so it integrates to 1
K = K / sum(K(:));

% Do filtering
filtered = imfilter3d(image,K);