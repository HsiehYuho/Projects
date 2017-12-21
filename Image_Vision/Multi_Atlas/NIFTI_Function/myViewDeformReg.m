function myViewDeformReg(fixed,moving,ux,uy,uz,spacing,crosshair,crange)
% myViewAffineReg: Visualize registration between two images
% usage:
%         myViewAffineReg(fixed,moving,A,b,spacing,crosshair,crange)
% required parameters:
%         fixed                   3D image volume (fixed)
%         moving                  3D image volume (moving)
%         ux,uy,uz                Displacement field
%         spacing                 3x1 vector of voxel spacings
% optional parameters:
%         crosshair               3x1 vector giving the crosshair position.
%                                 Defaults to the center of the image.
%         crange                  1x2 vector giving the range of intensity
%                                 to be displayed. Defaults to [imin imax]
%                                 where imin and imax are the minimum and 
%                                 maximum intensity in the image.

if (nargin < 9 || isempty(cmap))
    cmap = 'gray';
end

if (nargin < 8 || isempty(crange))
    crange = [min(fixed(:)) max(fixed(:))];
end

if (nargin < 7 || isempty(crosshair))
    crosshair = round(size(fixed) / 2);
end

% Clear the display
clf;

% Define a lambda function for short hand for interpn
[px,py,pz] = ndgrid(1:size(fixed,1), 1:size(fixed,2), 1:size(fixed,3));
warp=@(J,ux,uy,uz)(interpn(J, px + ux, py + uy, pz + uz, '*linear', 0));

% Reslice the moving image into the space of the fixed image
resliced = warp(moving, ux, uy, uz);

% Extract contours from the moving image
mv_samp = moving(round(linspace(1,numel(moving),5000)))';
[~,ctr] = kmeans(mv_samp(mv_samp > 0), 3);
cont = conv(sort(ctr), [0.5,0.5], 'valid');

% Extract the three slice planes from the image
S{1} = squeeze(fixed(:,:,crosshair(3)));
S{2} = squeeze(fixed(crosshair(1),:,:));
S{3} = squeeze(fixed(:,crosshair(2),:));

R{1} = squeeze(resliced(:,:,crosshair(3)));
R{2} = squeeze(resliced(crosshair(1),:,:));
R{3} = squeeze(resliced(:,crosshair(2),:));

% Extract the slice planes from the field U
gx{1} = squeeze(ux(:,:,crosshair(3))); gy{1} = squeeze(uy(:,:,crosshair(3)));
gx{2} = squeeze(uy(crosshair(1),:,:)); gy{2} = squeeze(uz(crosshair(1),:,:));
gx{3} = squeeze(ux(:,crosshair(2),:)); gy{3} = squeeze(uz(:,crosshair(2),:));

% Store the slicing dimension for each view
sx{1} = 1; sy{1} = 2; sz{1} = 3;
sx{2} = 2; sy{2} = 3; sz{2} = 1;
sx{3} = 1; sy{3} = 3; sz{3} = 2;

% Plot each of the slices
for i = 1:3
    
    % Select the proper subplot
    subplot(2,3,i);
    
    % Plot the fixed image as grayscale
    imagesc(S{i}');
        
    % Colormap
    colormap(cmap);
        
    % Set the limits of the color axis
    caxis(crange);
    
    % Plot contours from the resliced image
    hold on; contour(R{i}', cont, 'g'); hold off;
      
    % Reverse the axis directions
    set(gca, 'XDir', 'reverse', 'YDir', 'normal');
        
    % Get the directions in the volume corresponding to slice x and y
    x = sx{i}; y = sy{i};
    
    % Plot the x cross-har
    line([crosshair(x) crosshair(x)], [0.5 size(fixed,y)-0.5],'Color','r');
    line([0.5 size(fixed,x)-0.5], [crosshair(y) crosshair(y)],'Color','r');

    % Set the aspect ratio
    daspect([1/spacing(x) 1/spacing(y) 1]);
    
    % Set the title
    title(sprintf('%c = %d','x'+sz{i}-1,crosshair(sz{i})));
    
    % Plot the grid
    subplot(2,3,i+3);
    
    % Plot the difference image
    imagesc(S{i}' - R{i}');
    caxis([-crange(2),crange(2)])
    
    hold on;
    gridplot(gx{i}',gy{i}',4,4);
    hold off;
    
    % Set the aspect ratio
    daspect([1/spacing(x) 1/spacing(y) 1]);
    
    % Reverse the axis directions
    set(gca, 'XDir', 'reverse', 'YDir', 'normal');
        


end





%text(0.1, 0.8, sprintf('xhair=[%d %d %d]\ndimen=[%d %d %d]\n', ...
%    crosshair, size(fixed)),'FontSize', 12, 'FontName', 'FixedWidth');
%axis off;