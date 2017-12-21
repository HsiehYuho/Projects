function myView(image,spacing,crosshair,crange,cmap)
% myView: Display orthogonal slices through a 3D image
% usage:
%         myView(image,spacing,crosshair,crange,cmap)
% required parameters:
%         image                   3D image volume
%         spacing                 3x1 vector of voxel spacings
% optional parameters:
%         crosshair               3x1 vector giving the crosshair position.
%                                 Defaults to the center of the image.
%         crange                  1x2 vector giving the range of intensity
%                                 to be displayed. Defaults to [imin imax]
%                                 where imin and imax are the minimum and 
%                                 maximum intensity in the image.
%         cmap                    String, giving the name of the color map
%                                 to use. Defaults to 'gray'

if (nargin < 5 || isempty(cmap))
    cmap = 'gray';
end

if (nargin < 4 || isempty(crange))
    crange = [min(image(:)) max(image(:))];
end

if (nargin < 3 || isempty(crosshair))
    crosshair = round(size(image) / 2);
end

% Clear the display
clf;

% Extract the three slice planes from the image
S{1} = squeeze(image(:,:,crosshair(3)));
S{2} = squeeze(image(crosshair(1),:,:));
S{3} = squeeze(image(:,crosshair(2),:));

% Store the slicing dimension for each view
sx{1} = 1; sy{1} = 2; sz{1} = 3;
sx{2} = 2; sy{2} = 3; sz{2} = 1;
sx{3} = 1; sy{3} = 3; sz{3} = 2;

% Plot each of the slices
for i = 1:3
    
    % Select the proper subplot
    subplot(2,2,i);
    
    % Flip left and right
    imagesc(S{i}');
  
    % Reverse the axis directions
    set(gca, 'XDir', 'reverse', 'YDir', 'normal');
    
    % Colormap
    colormap(cmap);
        
    % Set the limits of the color axis
    caxis(crange);
    
    % Get the directions in the volume corresponding to slice x and y
    x = sx{i}; y = sy{i};
    
    % Plot the x cross-har
    line([crosshair(x) crosshair(x)], [0.5 size(image,y)-0.5],'Color','b');
    line([0.5 size(image,x)-0.5], [crosshair(y) crosshair(y)],'Color','b');

    % Set the aspect ratio
    daspect([1/spacing(x) 1/spacing(y) 1]);
    
    % Set the title
    title(sprintf('%c = %d','x'+sz{i}-1,crosshair(sz{i})));

end

% Some annotation in the empty space
subplot(2,2,4);
text(0.1, 0.8, sprintf('xhair=[%d %d %d]\ndimen=[%d %d %d]\n', ...
    crosshair, size(image)),'FontSize', 12, 'FontName', 'FixedWidth');
axis off;

% Display the colorbar in the lower right corner
caxis(crange);
colormap(cmap);
colorbar('South');