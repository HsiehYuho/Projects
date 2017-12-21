function [gdsc,dsc] = myDiceOverlap(S1,S2,L)
%myDiceOverlap Dice similarity coefficient between segmentations
%    [gdsc,dsc] = myDiceOverlap(S1,S2)   Compute Dice coefficient between
%                                        multi-label segmentations S1 and S2
%
%    [gdsc,dsc] = myDiceOverlap(S1,S2,L) Compute Dice coefficient for the 
%                                        set of labels in vector L
%
%    This function computes metrics of similarity between two multi-label
%    segmentations stored in 3D arrays S1 and S2. The following metrics
%    are computed:
%
%        For each label k in L, Dice similarity coefficient (DSC). DSC is
%        defined as the ratio of the number of voxels where S1 and S2 both
%        have label k, to the average number of voxels with label k in the
%        two segmentations. DSC has range between 0 and 1, with 1 being 
%        perfect overlap. DSC is retuned in array dsc.
%
%        A summary measure called Generalized DSC is computed. GDSC is a
%        measure of relative overlap across all labels in L. GDSC is also
%        between 0 and 1

% Make sure we have a label set
if (nargin < 3 || isempty(L))
    
    % Compute the label set as union of all labels present
    L = unique([S1(:); S2(:)]); 
    
end

% Take a brute force approach
P1=S1(:);
P2=S2(:);

% Initialize dsc
if nargout > 1
    
    dsc = nan(length(L),1);

    for j = 1:length(L)

        B1 = (P1 == L(j));
        B2 = (P2 == L(j));
        n1 = sum(B1);
        n2 = sum(B2);
        n12 = sum(B1 & B2);
        dsc(j) = 2 * n12 / (n1 + n2);

    end
    
end

% Compute the gdsc
B1 = (P1 > 0);
B2 = (P2 > 0);
B12 = (P1 == P2) & (B1 & B2);
gdsc = 2 * sum(B12) / (sum(B1) + sum(B2));



