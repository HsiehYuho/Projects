clc;
clear;
close all;
%% RVOT Multi-Altlas Segmentation

% add the path
addpath('Test_Data/Images/Img_Nii');
addpath('Test_Data/Images/Label_Nii');
addpath('Test_Data/Images/Seg_Resample_Nii');
addpath('NIFTI_Function/');
addpath('Helper_Function/');


% Configuration
clear_all_intermediate_output = 0;
turn_on_validate = 1;
atlas_min = 3;
atlas_max = 3;

%Input file path
input_img = 'Input/Test_Img.nii';
intput_label = 'Input/Test_Label.nii';

% Read the file dir 
imgs_files = dir('Test_Data/Images/Img_Nii/*_Image.nii');
segs_files = dir('Test_Data/Images/Seg_Resample_Nii/*_Seg_Resample.nii');
labels_files = dir('Test_Data/Images/Label_Nii/*_Label.nii');
num = length(imgs_files);

% Initialize images, segmentation and labels cells 
imgs_path = cell(1,num);
segs_path = cell(1,num);
labels_path = cell(1,num);
labels_points = cell(1,num);

% Read the nii & seg & label file 
for i = 1 : num 
    img_filename = strcat('Test_Data/Images/Img_Nii/',imgs_files(i).name);
    seg_filename = strcat('Test_Data/Images/Seg_Resample_Nii/',segs_files(i).name);    
    label_filename = strcat('Test_Data/Images/Label_Nii/',labels_files(i).name);
    imgs_path{i} = img_filename;
    segs_path{i} = seg_filename;
    labels_path{i} = label_filename;
end
disp('Finish reading data_path');


%% Change the label landmarks to points 
for i = 1 : num
    label_file = labels_path{i};
    [vox,phy] = convert_nii2txt2point(label_file);
    points = struct('vox',vox,'phy',phy);
    labels_points{i} = points;
end 
% Input point vox and phy
[intput_vox, intpu_phy] = convert_nii2txt2point(intput_label);
disp('finish converting landmark to point');
 
% Calculate Affine transformation Matrix
H_n_to_target = {};
for i = 1 : num
    H_n_to_target{i} = affine_tform(intpu_phy,labels_points{i}.phy); 
end
disp('Finish calculating H matrix');


%% Apply affine & Deform transformation on moving img & moving seg 
dsc_val=[];
for atlas_num = atlas_min : atlas_max
    for i = 1 : atlas_num
        % Affine Transformation on images
        t_img = 'Input/Test_Img.nii';
        m_img = imgs_path{i};
        aff_img_m2t = strcat('./Test_Data/Images/Affine_Nii/',num2str(i),'_to_target_Img_Aff.nii');
        if exist(aff_img_m2t, 'file') ~= 2
            affine_trans_reslice(t_img,m_img,inv(H_n_to_target{i}),aff_img_m2t);
        end

        % Affine Transformation on segs
        m_seg = segs_path{i};
        aff_seg_m2t = strcat('./Test_Data/Images/Affine_Nii/',num2str(i),'_to_target_Seg_Aff.nii');
        if exist(aff_seg_m2t, 'file') ~= 2
           affine_trans_reslice(t_img,m_seg,inv(H_n_to_target{i}),aff_seg_m2t);
        end
        

        % Deform Transformation on segs
        aff_def_seg = strcat('./Test_Data/Images/Affine_Deform_Nii/',num2str(i),'_to_target_Seg_Aff_Def.nii');
        if exist(aff_def_seg, 'file') ~= 2
            deform_trans(t_img,aff_img_m2t,aff_seg_m2t,aff_def_seg);
        end
    end 
    disp('Finish affine and deform transformation');

    %% Using majority voting to get the "predict segmentation"
    aff_def_seg_files = dir('Test_Data/Images/Affine_Deform_Nii/*_Seg_Aff_Def.nii');
    num_seg = length(aff_def_seg_files);
    segs = cell(1,num_seg);
    for i = 1 : num_seg
        filename = strcat('Test_Data/Images/Affine_Deform_Nii/',aff_def_seg_files(i).name);
        [segs{i},seg_space] = myReadNifti(filename);
    end

    predict_seg_path = strcat('Output/predict_seg.nii');
    predict_seg_rot_path = strcat('Output/predict_seg_rot.nii');

    majority_voting(segs,seg_space,predict_seg_path,predict_seg_rot_path);
    disp('Finish majority voting');

    %% Validation based on diceoverlay score
    if(turn_on_validate)
        ground_truth_seg_path = 'Input/Test_Seg.nii';
        [gdsc,dsc] = dice_overlap(ground_truth_seg_path,predict_seg_path);
        fprintf('DSC: %6.4f \n',dsc(1));
        disp('Finish registration');
        dsc_val = [dsc_val,dsc(1)];
    end 
end
%% plot dice overlaps
X = [];
X = [1:atlas_max, X];
f = figure();
scatter(X, dsc_val); xlabel('Number atlases used'); set(gca, 'xlim',[0, atlas_max], 'xtick', [0:1:atlas_max], 'ylim', [0, 1]); ylabel('Dice overlap'); title('DSC between ground truth and mutli-atlas segmentation');
saveas(f, 'Output/DSC_Plot.png');
if(clear_all_intermediate_output)
    clear_all_imgs();
end
disp('done');
