% x
x = [0 1 2 3 4 5 6 7];

% y
y1 = [0.030957,0.065537,0.338409,1.251227,8.702084,16.536641,35.988448,69.523914,101.646112,259.414206,460.156432,708.494997,1092.863658,1100.619964,1134.847164,1150.989131];
y2 = [0.028639,0.060845,0.326859,1.233974,8.610321,16.515673,35.924659,69.659044,117.043312,339.427485,609.680681,1016.151606,1566.086025,1537.677702,2549.450516,1828.593586];


% draw
hold on
plot(x, y1(2:2:16), 'r-', 'LineWidth', 1);
plot(x, y2(2:2:16), 'b-', 'LineWidth', 1);
hold off

legend('FUSE','Kernel by pass');

xticks(x);
xticklabels({'64B','256B','1KB','4KB','16KB','64KB','256KB','1MB'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('I/O bandwidth (MB/S)','fontweight','bold','FontSize',25);
xlabel('I/O request size','fontweight','bold','FontSize',25);
xlim([0 7]);