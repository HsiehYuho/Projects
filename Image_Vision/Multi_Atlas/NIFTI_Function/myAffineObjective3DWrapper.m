function [E,grad_E]=myAffineObjective3DWrapper(x,I,J)

% Unpack x into A and b
A=reshape(x(1:9),[3,3]);
b=reshape(x(10:12),[3,1]);

% Compute objective 
if nargout > 1
    [E,dE_dA,dE_db] = myAffineObjective3D(A,b,I,J);

    % Pack partial derivatives into a flat gradient vector
    grad_E = [dE_dA(:); dE_db];
else
    
    E = myAffineObjective3D(A,b,I,J);
    
end