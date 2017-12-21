function [E,dE_dA,dE_db] = myAffineObjective3D(A, b, I, J)
% myAffineObjective3D  Compute the energy functoin for affine registration
%
%     E = myAffineObjective3D(A, b, I, J)
%
%     Computes the energy function E for affine registration between fixed
%     image I and moving image J, with affine transformation given by the
%     3x3 matrix A and 3x1 vector b. 
%     
%     [E,dE_dA,dE_db] = myAffineObjective3D(A, b, I, J)
%
%     Also computes the partial derivaties of E with respect to the elements
%     of A and b. Thus dE_dA is a 3x3 matrix, and dE_db is a 3x1 vector.
%

% Use ndgrid to generate a coordinate grid
    [x,y,z] = size(I);  % 96 * 116 * 128  
    [Xp,Yp,Zp] = ndgrid(1:x,1:y,1:z); % x = 96 * 116 * 128
    Xpnew = Xp(:);
    Ypnew = Yp(:);
    Zpnew = Zp(:);
    coords = [Xpnew,Ypnew,Zpnew];
    
% Apply the affine transformation in A,b to the coordinate grid
    n = size(Xpnew,1);
    Bnew = repmat(b,1,n);
    Txp = A*coords.'+Bnew;
    % Txp query point, v is moving 

% Resample the moving image
    tm = interpn(1:x,1:y,1:z,J,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
    J_int = reshape(tm,[x,y,z]);
% Compute the objective function value (E)
    idiff = I - J_int;
    E = sum(sum(sum(idiff .^ 2)));    
% Compute the partial derivatives only if requested
    if nargout > 1
        % Your code to compute the elements of dE_dA and dE_db
        [dJ_dy, dJ_dx, dJ_dz] = gradient(J);
        tm_x = interpn(1:x,1:y,1:z,dJ_dx,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
        dx = reshape(tm_x,[x,y,z]);
        tm_y = interpn(1:x,1:y,1:z,dJ_dy,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
        dy = reshape(tm_y,[x,y,z]);
        tm_z = interpn(1:x,1:y,1:z,dJ_dz,Txp(1,:),Txp(2,:),Txp(3,:),'*linear',0);
        dz = reshape(tm_z,[x,y,z]);

        dx = -2 * idiff .* dx;
        dy = -2 * idiff .* dy;
        dz = -2 * idiff .* dz;
        dE_dA(1,1) = sum(sum(sum(dx .* Xp)));
        dE_dA(2,1) = sum(sum(sum(dy .* Xp)));
        dE_dA(3,1) = sum(sum(sum(dz .* Xp)));
        dE_dA(1,2) = sum(sum(sum(dx .* Yp)));
        dE_dA(2,2) = sum(sum(sum(dy .* Yp)));
        dE_dA(3,2) = sum(sum(sum(dz .* Yp)));
        dE_dA(1,3) = sum(sum(sum(dx .* Zp)));
        dE_dA(2,3) = sum(sum(sum(dy .* Zp)));
        dE_dA(3,3) = sum(sum(sum(dz .* Zp)));        
        
        dE_db(1,1) = sum(dx(:));
        dE_db(2,1) = sum(dy(:));
        dE_db(3,1) = sum(dz(:));
    end
end
