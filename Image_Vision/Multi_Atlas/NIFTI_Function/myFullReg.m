function p = myFullReg(I,J,param)
%myFullReg 3D Image Affine and Deformable Registration.
%   p = myFullReg(I,J,param)    Performs affine and deformable
%                               registration.
%
%   myFullReg performs affine and deformable registration between a fixed
%   image I and a moving image J. The param structure should be generated
%   using myFullRegInit function.
%
%   The return value p is a struct containing:
%     affine        Structure containing affine results and stats:
%        A,b        Affine transform matrix and vector
%        E_init     Dissimilarity metric value before optimization
%        E_final    Dissimilarity metric value after optimization
%        time       Time elapsed in optimization
%     deform        Structure containing deformable results and stats:
%        ux,uy,uz   Dispacement field for deformable registation
%        E_init     Dissimilarity metric value before optimization
%        E_final    Dissimilarity metric value after optimization
%        time       Time elapsed in optimization
%     param         Copy of the param struct passed in to the function

    
    % Prepare return structure
    p = struct();
    p.affine = struct();
    p.deform = struct();
    p.param = param;

    % Smooth the input images
    I_sm=myGaussianLPF(I,param.sigma);
    J_sm=myGaussianLPF(J,param.sigma);

    % Compute the initial objective value
    f_opt = @(x)(myAffineObjective3DWrapper(x, I_sm, J_sm));
    x_init = [param.affine.init_A(:); param.affine.init_b(:)];
    p.affine.E_init = f_opt(x_init);

    % Plot function for affine registration
    if param.flags.plot_iter ~= 0
        plot_fn = @(x,~,~)(myAffineOptCallback(x,I_sm,J_sm,param.img.I_spc));
    else
        plot_fn = [];
    end
    
    % Do we print intermediate results?
    if param.flags.print_iter ~= 0
        display_mode = 'iter';
    else
        display_mode = 'off';
    end
    
    % Perform affine registration
    opt=optimoptions(...
        'fminunc','Display',display_mode,...
        'GradObj','on',...
        'OutputFcn',plot_fn,...
        'MaxIter',param.affine.n_iter);

    tic; 
    [x_opt,p.affine.E_final] = fminunc(f_opt, x_init, opt);
    p.affine.time = toc;

    % Extract the final matrix
    p.affine.A = reshape(x_opt(1:9),3,3);
    p.affine.b = x_opt(10:12);

    % Prepare the deformable registration
    J_sm_aff=myTransformImage(I,J_sm,p.affine.A,p.affine.b);
    ux=zeros(size(I)); uy=zeros(size(I)); uz=zeros(size(I));

    % Compute the initial energy - affine function does the job quicker
    p.deform.E_init = myAffineObjective3D(eye(3), zeros(3,1), I_sm, J_sm_aff);

    % Perform deformable registration in a loop
    tic;
    %% Chance at here 40 -> 5
    for i = 1:40

        % Single registration step using myGreedyRegUpdate()
        [E,ux,uy,uz]=myGreedyRegUpdate(ux,uy,uz,I_sm,J_sm_aff,...
            param.deform.sigma, param.deform.tau, param.deform.rho);

        % Print objective for iteration
        if param.flags.print_iter ~= 0
            fprintf('Deform Iter %03d   E = %10.4g\n', i, E)
        end

        % Plot iteration
        if param.flags.plot_iter ~= 0
            myViewDeformReg(I_sm,J_sm_aff,ux,uy,uz,param.img.I_spc);
            getframe();
        end

    end
    p.deform.time = toc;
    p.deform.E_final = E;

    % Store the transform
    p.deform.ux = ux;
    p.deform.uy = uy;
    p.deform.uz = uz;

end


% Callback function for plotting
function stop = myAffineOptCallback(x,I,J,I_spc)

    A = reshape(x(1:9),3,3);
    b = x(10:12);
    myViewAffineReg(I,J,A,b,I_spc);
    getframe();
    stop = false;

end


   