% Calculate the ground truth and predict overlap percentage
% Input: seg_ground_truth is the manual segmenation ground truth path
%        seg_predict is the segmentation file path after majority voting 
% Output: GDSC and DSC score, reference to myDiceOverlap file
function [gdsc,dsc] = dice_overlap(seg_ground_truth,seg_predict)
    [ground_truth,~] = myReadNifti(seg_ground_truth);
    [predict,~] = myReadNifti(seg_predict);
    ground_truth = ground_truth < 0.5;
    predict = predict < 0.5;
    [gdsc,dsc] = myDiceOverlap(ground_truth,predict,[0,1]);
end