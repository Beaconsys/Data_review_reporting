% x
x = [0 1 2 3 4 5];

% y
y1 = [13,32,101,193,263,313];
y2 = [8,16,35,61,77,91];
y3 = [5,12,24,52,54,56];
y4 = [3,8,14,25,25,27];
y5 = [3,7,10,15,17,20];

% draw
hold on
plot(x, y1, 'r-', 'LineWidth', 2);
plot(x, y2, 'c--', 'LineWidth', 2);
plot(x, y3, 'b-', 'LineWidth', 2);
plot(x, y4, 'r--', 'LineWidth', 2);
plot(x, y5, 'c-', 'LineWidth', 2);
hold off


legend('(0,1)','[1,4)','[4,12)','[12,96)','>=96','Location','best');

xticks(x)
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('No. of anomalys','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);