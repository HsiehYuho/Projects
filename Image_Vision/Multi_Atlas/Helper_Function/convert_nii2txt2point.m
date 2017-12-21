% Convert nii file landmark file into txt 
% Input: nii_seg is the label of the landmark 
% Output: points in voxel coordinate and physical coordinate
function [landmark_vox_array,landmark_phy_array] = convert_nii2txt2point(nii_seg)
    % Convert landmark to point txt file 
    txt = 'tmp.txt';
    c3d = '/Applications/Convert3DGUI.app/Contents/bin/c3d';
    option = '-split -foreach -centroid -endfor >>';
    format = '%s %s %s %s';
    save_exe = sprintf(format,c3d,strcat(nii_seg),option,txt);
    
    system(save_exe);  
    
    % Call another function to convert txt to points
    [landmark_vox_array,landmark_phy_array] = txt_to_point(txt);
end
