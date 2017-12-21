% Takes in a set of points (landmarks), where the first 6 landmarks are
% boundaries for the image (+/-X, +/-Y, +/-Z), and returns the cropped
% image. 
% https://www.mathworks.com/matlabcentral/answers/225333-cropping-a-3d-matrix
% points are n*3 matrix
% image is a image after my_nii_read 

function cropped = crop3dimg(points, image)

if size(points) < 6
    error('not enough data points for cropping');
end

% extract boundary points from landmarks
boundaries = points(1:6,:);

%find max and min x, y, z values
maxX = ceil(max(boundaries(:,1))) ;
maxY = ceil(max(boundaries(:,2)));
maxZ = ceil(max(boundaries(:,3)));

minX = floor(min(boundaries(:,1)));
minY = floor(min(boundaries(:,2)));
minZ = floor(min(boundaries(:,3)));

%crop original image, NOTE THAT Y AND X ARE FLIPPED!
cropped = image(minZ:maxZ, minY:maxY, minX:maxX);

end
