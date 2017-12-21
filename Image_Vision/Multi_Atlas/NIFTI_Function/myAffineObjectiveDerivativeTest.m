function myAffineObjectiveDerivativeTest(A, b, I, J, eps)
%myAffineObjectiveDerivativeTest Test affine objective derivatives
%
%   myAffineObjectiveDerivativeTest(A, b, I, J, eps)
%
%   Call this function to test the accuracy of analytic derivatives
%   computed by myAffineObjective. Inputs I, J are the fixed and moving
%   images, and A, b specify the affine transform for which you want to
%   test the derivatives. The parameter eps is the epsilon for central
%   finite difference approximation.
%
%   The comparison of derivatives is printed out, and no output is
%   returned.


[~,dE_dA,dE_db]=myAffineObjective3D(A,b,I,J);
for i=1:3
    for j=1:3
        A1 = A; A1(i,j) = A(i,j) - eps;
        A2 = A; A2(i,j) = A(i,j) + eps;
        E1=myAffineObjective3D(A1,b,I,J);
        E2=myAffineObjective3D(A2,b,I,J);
        num_deriv=(E2-E1) / (2*eps);
        fprintf('dE/dA(%d,%d) : Anl.D. = %10.4g  Num.D. = %10.4g  Err = %10.4f\n',...
            i,j,dE_dA(i,j),num_deriv,abs((num_deriv - dE_dA(i,j))/dE_dA(i,j)));        
    end
end

for i=1:3
    b1 = b; b1(i) = b(i) - eps;
    b2 = b; b2(i) = b(i) + eps;
    E1=myAffineObjective3D(A,b1,I,J);
    E2=myAffineObjective3D(A,b2,I,J);
    num_deriv=(E2-E1) / (2*eps);
    fprintf('dE/db(%d)   : Anl.D. = %10.4g  Num.D. = %10.4g  Err = %10.4f\n',...
        i,dE_db(i),num_deriv,abs((num_deriv - dE_db(i))/dE_db(i)));        
end