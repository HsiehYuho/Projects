function [E,Ux,Uy,Uz] = myGreedyRegUpdate(ux,uy,uz,I,J,sigma,tau,rho)
% myGreedyRegUpdate : perform an iteration of greedy registration
%
%    [E,Ux,Uy,Uz] = myGreedyRegUpdate(ux,uy,uz,I,J,sigma,tau,rho)
%
%    Performs one iterative update in the greedy deformable registration
%    algorithm, with I as the fixed image, J as the moving image. Other
%    parameters are:
%
%      ux, uy, uz          The x,y,z components of the displacement field
%                          u^t. Each is of the same dimensions as I.
%      sigma               Smoothing applied to the gradient of the energy
%      tau                 Smoothing applied to the composed displacement
%      rho                 Normalization factor
%
%    Return values:
%      E                   The value of MSID metric for u^t
%      Ux,Uy,Uz            The x,y,z components of the displacement field
%                          u^(t+1)
% Use ndgrid to generate a coordinate grid
    [x,y,z] = size(I);  % 96 * 116 * 128  
    [Xp,Yp,Zp] = ndgrid(1:x,1:y,1:z); % x = 96 * 116 * 128
    Xpnew = Xp(:);
    Ypnew = Yp(:);
    Zpnew = Zp(:);
    coords = [Xpnew,Ypnew,Zpnew];
    
% Apply the affine transformation in 1,ux,uy,uz to the coordinate grid
%     n = size(Xpnew,1);
    Bnew = [ux(:),uy(:),uz(:)]';
    Txp = coords.'+Bnew;
    % Txp query point, v is moving 

% Resample the moving image
    tm = interpn(1:x,1:y,1:z,J,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    J_int = reshape(tm,[x,y,z]);

% Compute the Energy 
    E = sum(sum(sum((I-J_int).^2)));
    
% compute the grad
    [dJ_dy, dJ_dx, dJ_dz] = gradient(J);
    idiff = I - J_int;
    
    tm_x = interpn(1:x,1:y,1:z,dJ_dx,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    gx = reshape(tm_x,[x,y,z]);

    tm_y = interpn(1:x,1:y,1:z,dJ_dy,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    gy = reshape(tm_y,[x,y,z]);
    
    tm_z = interpn(1:x,1:y,1:z,dJ_dz,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    gz = reshape(tm_z,[x,y,z]);
    
    wx = 2 * idiff .* gx;
    wy = 2 * idiff .* gy;
    wz = 2 * idiff .* gz;
    
    % Compute the gaussian 5c
    wx_Gau = myGaussianLPF(wx, sigma);
    wy_Gau = myGaussianLPF(wy, sigma);
    wz_Gau = myGaussianLPF(wz, sigma);
    
    % Doing the norm 5d
    norm_matrix = (wx_Gau.^2+wy_Gau.^2+wz_Gau.^2);
    norm_max = sqrt(max(norm_matrix(:)));
    
    w_nrm_x = wx_Gau;
    w_nrm_y = wy_Gau;
    w_nrm_z = wz_Gau;

    if(norm_max > rho)
        w_nrm_x = (rho/norm_max)* wx_Gau;
        w_nrm_y = (rho/norm_max)* wy_Gau;
        w_nrm_z = (rho/norm_max)* wz_Gau;
    end
    
    % Compute the update 5e 
    Bnew = [w_nrm_x(:),w_nrm_y(:),w_nrm_z(:)]';
    Txp = coords.'+Bnew;
    tm_x = interpn(1:x,1:y,1:z,ux,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    ux_new = reshape(tm_x,[x,y,z]);
    tm_y = interpn(1:x,1:y,1:z,uy,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    uy_new = reshape(tm_y,[x,y,z]);
    tm_z = interpn(1:x,1:y,1:z,uz,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    uz_new = reshape(tm_z,[x,y,z]);
    
    Ux =  myGaussianLPF(w_nrm_x+ux_new,tau);
    Uy =  myGaussianLPF(w_nrm_y+uy_new,tau);
    Uz =  myGaussianLPF(w_nrm_z+uz_new,tau);

    
    
