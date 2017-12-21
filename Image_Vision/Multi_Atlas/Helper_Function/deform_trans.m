% Apply deform transformation on candidate seg image
% Input: moving is the atlas image after affine
%        target is the target image
%        aff_seg_path is path of the atlas segmentation after affine 
%        aff_def_seg_path is the path of atlas segmentation after affine and deform

function deform_trans(target,moving,aff_seg_path,aff_def_seg_path)
    [target_img,~] = myReadNifti(target);
    [moving_img,~] = myReadNifti(moving);
    sigma = sqrt(3);
    I = myGaussianLPF(target_img, sigma);
    J = myGaussianLPF(moving_img, sigma);

    ux = zeros(size(I));
    uy = zeros(size(I));
    uz = zeros(size(I));
    tau = sqrt(0.5);
    rho = 0.5;
    T = 20; % 50 is more appropiate
    for i = 1 : T
        [E,ux,uy,uz] = myGreedyRegUpdate(ux,uy,uz,I,J,sigma,tau,rho);
       % fprintf('Iter: %d , E: %e \n',i,E);
    end

    % Apply ux,uy,uz on seg and save it 
    [aff_seg,aff_seg_space] = myReadNifti(aff_seg_path);
    [px,py,pz] = ndgrid(1:size(I,1), 1:size(I,2), 1:size(I,3));
    warp=@(J,ux,uy,uz)(interpn(J, px + ux, py + uy, pz + uz, '*linear', 0));
    aff_def_seg = warp(aff_seg, ux, uy, uz);    
    aff_def_seg_rot = rot90(permute(aff_def_seg,[2,3,1]),2);

%     myWriteNifti(aff_def_seg_path,aff_def_seg,aff_seg_space);
    myWriteNifti(aff_def_seg_path,aff_def_seg_rot,aff_seg_space);

end



