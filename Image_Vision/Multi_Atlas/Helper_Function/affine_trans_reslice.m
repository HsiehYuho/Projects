% Input: a 3D image img path 
%      : a homogeneous backward affine transformation matrix H
% Ouput: a new image path after affine transformtation

function affine_trans_reslice(target,moving, invH, new_img_path)   
%   Create invH txt file in system
    tmp_H_txt = './invH.txt';
    fileID = fopen(tmp_H_txt,'w');
    fprintf(fileID,'%f %f %f %f  \r\n',invH');
    fclose(fileID);
    
    c3d = '/Applications/Convert3DGUI.app/Contents/bin/c3d';
    interpolation = '-interpolation NearestNeighbor';
    reslice = '-reslice-matrix ./invH.txt -o';
    format = '%s %s %s %s %s %s %s';
    affine_trans_cmd = sprintf(format,c3d,interpolation,target,moving,reslice,new_img_path);
    
    % Execute the in terminal
    system(affine_trans_cmd);  

    % Delete invH txt file 
    delete_format = '%s %s';
    delete_exe = sprintf(delete_format,'rm',tmp_H_txt);
    system(delete_exe);
    
end