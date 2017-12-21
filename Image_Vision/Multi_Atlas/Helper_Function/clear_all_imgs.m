% clear all intermediate files function clears all the intermediate files
% except prediction segmentation

function clear_all_imgs()
    delete_format = '%s %s';
    
    % Delete aff and deform segs
    aff_def = 'Test_Data/Images/Affine_Deform_Nii/*.nii';
    delete_exe = sprintf(delete_format,'rm',aff_def);
    system(delete_exe);
    
    % Delete Affine imgs and segs
    aff_img_seg = 'Test_Data/Images/Affine_Nii/*.nii';
    delete_exe = sprintf(delete_format,'rm',aff_img_seg);
    system(delete_exe);
end
