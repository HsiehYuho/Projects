% File: Do the majority voting
% Input: segs is an cell array
%        seg_space should be the same as target image sapce
%        predict_seg_path is for validation
%        predict_seg_rot_path is for viewing

function majority_voting(segs,seg_space,predict_seg_path,predict_seg_rot_path)
    S = myMajorityVote(segs);
    S_rot = rot90(permute(S,[2,3,1]),2);    
    myWriteNifti(predict_seg_path,S,seg_space);
    myWriteNifti(predict_seg_rot_path,S_rot,seg_space);

end
