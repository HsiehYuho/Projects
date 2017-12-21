function myViewAffineReg(fixed,moving,A,b,spacing,crosshair,crange)
% myViewAffineReg: Visualize registration between two images
% usage:
%         myViewAffineReg(fixed,moving,A,b,spacing,crosshair,crange)
% required parameters:
%         fixed                   3D image volume (fixed)
%         moving                  3D image volume (moving)
%         A,b                     Affine transform
%         spacing                 3x1 vector of voxel spacings
% optional parameters:
%         crosshair               3x1 vector giving the crosshair position.
%                                 Defaults to the center of the image.
%         crange                  1x2 vector giving the range of intensity
%                                 to be displayed. Defaults to [imin imax]
%                                 where imin and imax are the minimum and 
%                                 maximum intensity in the image.

if (nargin < 8 || isempty(cmap))
    cmap = 'gray';
end

if (nargin < 7 || isempty(crange))
    crange = [min(fixed(:)) max(fixed(:))];
end

if (nargin < 6 || isempty(crosshair))
    crosshair = round(size(fixed) / 2);
end

% Clear the display
clf;

% Reslice the moving image into the space of the fixed image
resliced = myTransformImage(fixed, moving, A, b, '*linear');

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

% Store the slicing dimension for each view
sx{1} = 1; sy{1} = 2; sz{1} = 3;
sx{2} = 2; sy{2} = 3; sz{2} = 1;
sx{3} = 1; sy{3} = 3; sz{3} = 2;

% Plot each of the slices
for i = 1:3
    
    % Select the proper subplot
    subplot(1,3,i);
    
    % Plot the fixed image as grayscale
    imagesc(S{i}');
    
    % Plot contours from the resliced image
    hold on; contour(R{i}', cont, 'g'); hold off;
    
    % Colormap
    colormap(cmap);
        
    % Set the limits of the color axis
    caxis(crange);
  
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

end

% % Plot a 3D visualization based on some background extraction
% subplot(2,2,4);
% pf = patch(isosurface(fixed, crange(1) * 0.9 + crange(2) * 0.1));
% isonormals(fixed, pf);
% set(pf,'FaceColor','yellow','EdgeColor','none');
% 
% hold on;
% pm = patch(isosurface(resliced, crange(1) * 0.9 + crange(2) * 0.1));
% isonormals(moving, pm);
% set(pm,'FaceColor','blue','EdgeColor','none');
% 
% daspect(spacing)
% view(3); axis vis3d; camlight; lighting gouraud;




%text(0.1, 0.8, sprintf('xhair=[%d %d %d]\ndimen=[%d %d %d]\n', ...
%    crosshair, size(fixed)),'FontSize', 12, 'FontName', 'FixedWidth');
%axis off;