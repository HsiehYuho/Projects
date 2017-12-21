% File: Transform the txt file into cell point 
% Input: filename is the path of the txt file with such form
% (Ex: ./Test_Data/LandMark/landmarks.txt)
% 
% CENTROID_VOX [87.5002, 127.5, 47.5002]
% CENTROID_MM [-4.4866, 0.186167, -12.5756]
% CENTROID_VOX [81.5, 72, 8.5]
% CENTROID_MM [-51.2868, 7.21765, 52.4639]
% CENTROID_VOX [51.5, 176, 16.5]
% 
% Output: contains two cell array with voxel coordinates and physical coordinates
% 

function [landmark_vox_array,landmark_phy_array] = txt_to_point(filename)
% Initialize the landmark points cell
landmark_vox = {};
landmark_phy = {};

% Reading the landmark txt file
fid = fopen(filename);
tline = fgetl(fid);
odd = true;
    while ischar(tline)
        startIdx = strfind(tline,'[');
        endIdx = strfind(tline,']');
        coord = tline(startIdx+1:endIdx-1);
        len = size(coord(:));
        comma = strfind(coord,',');
        xEnd = comma(1)-1;
        yEnd = comma(2)-1;
        zEnd = len(1);
        x = str2double(coord(1 : xEnd));
        y = str2double(coord(xEnd+3 : comma(2)-1));
        z = str2double(coord(yEnd+3 : zEnd));
        if(odd)
            landmark_vox(end+1) = {[x,y,z]};
        else
            landmark_phy(end+1) = {[x,y,z]};
        end
        odd = ~odd;
        tline = fgetl(fid);
    end
fclose(fid);

[length,~] = size(landmark_vox(:));
mat = cell2mat(landmark_vox);
landmark_vox_array = reshape(mat,[3,length])'; 
landmark_vox_array = landmark_vox_array(2:end,:,:);

[length,~] = size(landmark_phy(:));
mat = cell2mat(landmark_phy);
landmark_phy_array = reshape(mat,[3,length])'; 
landmark_phy_array = landmark_phy_array(2:end,:,:); 

% Delete the txt file 
rm = 'rm';
delete_format = '%s %s';
delete_exe = sprintf(delete_format,rm,filename);
    
system(delete_exe);


end


