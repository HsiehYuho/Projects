function gridplot(fx, fy, ix, iy)

[ny nx] = size(fx);

[mx my] = meshgrid(1:nx,1:ny);

newplot;
hold on;
axis([1 nx 1 ny]);

for k = 1:iy:ny
    line(mx(k,:) + fx(k,:), my(k,:) + fy(k,:),'Color',[0 0.75 0]);
end

for k = 1:ix:nx
    line(mx(:,k) + fx(:,k), my(:,k) + fy(:,k),'Color',[0 0.75 0]);
end

axis image;

hold off;
