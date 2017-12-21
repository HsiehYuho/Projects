function tform = affine_tform(targ, mov)
% Extracting the points that we need (1~6 is the boundary points)
targ = targ(7:end,:,:);
mov = mov(7:end,:,:);

% doing the padding one at the end of the matrix
num_points = size(targ,1);
padding = ones(num_points,1);
targ = [targ,padding];
mov = [mov,padding];
targ = targ';
mov = mov';


% First translate to origin
cm_targ = mean(targ,2);
cm_mov = mean(mov,2);

Ttrans_targ = trans(-cm_targ(1:3));
Ttrans_mov = trans(-cm_mov(1:3));

targ_trans = Ttrans_targ * targ;
xpts_trans = Ttrans_mov * mov;

% Second, optimize rigid transform parameters
x0 = [0 0 0 0 0 0];
f = @(x)mindist(x,targ_trans,xpts_trans,'rigid');
options = optimoptions('fminunc','MaxFunEvals',5000);
x = fminunc(f,x0,options);

Tt = trans(x(1:3));
Tx = rotx(x(4));
Ty = roty(x(5));
Tz = rotz(x(6));

Trigid = Tx * Ty * Tz * Tt;
xpts_rigid = Trigid * xpts_trans;

% Finally, optimize similarity transform parameters
x0 = [0 0 0 0 0 0 1];
f = @(x)mindist(x,targ_trans,xpts_rigid,'similarity');
x = fminunc(f,x0,options);

Tt = trans(x(1:3));
Tx = rotx(x(4));
Ty = roty(x(5));
Tz = rotz(x(6));
Ts = uniscale(x(7));

Tsim = Ts * Tx * Ty * Tz * Tt;

% Remember to apply inverse of target translation
Tt_targ_inv = trans(cm_targ(1:3));

% Composite output transformation
tform = Tt_targ_inv * Tsim * Trigid * Ttrans_mov;

end

function Dsum = mindist(x, targ, mov, flag)

% number of landmark points
np = size(mov,2);

% points to transform
xpts = mov;

switch flag
    case 'translate'
        
        % translational components
        tx = x(1);
        ty = x(2);
        tz = x(3);
        
        % transformation
        T = trans([tx ty tz]);
        
    case 'rigid'
        
        % translational components
        tx = x(1);
        ty = x(2);
        tz = x(3);
        
        % rotational components
        alpha = x(4);
        beta = x(5);
        gamma = x(6);
        
        % transformation matrix
        Tt = trans([tx ty tz]);
        Tx = rotx(alpha);
        Ty = roty(beta);
        Tz = rotz(gamma);
        
        T = Tx * Ty * Tz * Tt;
        
    case 'similarity'
        
        % translational components
        tx = x(1);
        ty = x(2);
        tz = x(3);
        
        % rotational components
        alpha = x(4);
        beta = x(5);
        gamma = x(6);
        
        % scaling component
        sc = x(7);
        
        % transformation matrix
        Tt = trans([tx ty tz]);
        Tx = rotx(alpha);
        Ty = roty(beta);
        Tz = rotz(gamma);
        Ts = uniscale(sc);
        
        T = Ts * Tx * Ty * Tz * Tt;
end
        
% Transform points
xpts_new = T * xpts;

% Compute distance to target points
D = 0;
for i = 1 : np
    D = D + pdist([xpts_new(1:3,i)'; targ(1:3,i)']);
end

Dsum = D;

end

% -------------------------------------------------------------------------

function Tx = rotx(alpha)

Tx = [1     0           0           0;
      0     cos(alpha)  -sin(alpha) 0;
      0     sin(alpha)  cos(alpha)  0;
      0     0           0           1];

end

function Ty = roty(beta)

Ty = [cos(beta)    0    sin(beta)   0;
      0            1    0           0;
      -sin(beta)   0    cos(beta)   0;
      0            0    0           1];
  
end

function Tz = rotz(gamma)

Tz = [cos(gamma)    -sin(gamma) 0   0;
      sin(gamma)    cos(gamma)  0   0;
      0             0           1   0;
      0             0           0   1];

end

function Tt = trans(x)

Tt = [1 0   0   x(1);
      0 1   0   x(2);
      0 0   1   x(3);
      0 0   0   1 ];
end

function Ts = uniscale(s)

Ts = [s 0 0 0;
      0 s 0 0;
      0 0 s 0;
      0 0 0 1];

end

