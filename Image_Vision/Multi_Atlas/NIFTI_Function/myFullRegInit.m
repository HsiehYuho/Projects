function param = myFullRegInit(I,J,I_spacing)
%myFullRegInit Generate default parameters for 3D registration
%   param = myFullRegInit(I,J,I_spacing)  Generate default parameters
%
%   Call this function before running myFullReg. It will generate a
%   default set of parameters that you can modify and pass to myFullReg.
    
% Generate default parameters
param = struct();

% Smoothing applied to images before registration
param.sigma = 1.0;

% Affine registration parameters
param.affine = struct();
param.affine.n_iter = 20;

% Affine registration initial guess
param.affine.init_A = eye(3);
param.affine.init_b = zeros(3,1);

% Deformable registration parameters
param.deform = struct();
param.deform.n_iter = 40;
param.deform.sigma = sqrt(3);
param.deform.tau = sqrt(0.5);
param.deform.rho = 0.5;

% General flags
param.flags = struct();
param.flags.print_iter = 1; % Print energy each iteration
param.flags.plot_iter = 0;  % Plot every iteration

% Information about the images 
param.img = struct();
param.img.I_size = size(I);
param.img.J_size = size(J);
param.img.I_spc = I_spacing;
    
end