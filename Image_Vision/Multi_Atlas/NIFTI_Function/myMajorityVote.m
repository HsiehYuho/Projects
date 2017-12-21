function S = myMajorityVote(segs)
%myMajorityVote Combine segmentation images using majority voting
%
%    S = myMajorityVote(segs)  Performs majority voting using segmentations
%                              passed in the cell array segs
%
%    To create the cell array, use this code:
%
%       segs=cell(5,1);
%       segs{1} = S1;
%       segs{2} = S2;
%       ...
%
%    All the segmentations must occupy the same space

% Flatten out the array
v = numel(segs{1});
s = length(segs);
D = zeros(v,s);
for i = 1:s
    D(:,i) = segs{i}(:);
end

% Use mode to vote
S = reshape(mode(D,2),size(segs{1}));
