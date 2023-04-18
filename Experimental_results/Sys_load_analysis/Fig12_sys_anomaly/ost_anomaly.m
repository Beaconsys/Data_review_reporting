% x
x = [0 1 2 3 4 5];

% y
y1 = [17,39,82,185,199,219];
y2 = [9,20,36,73,84,99];
y3 = [5,12,24,52,64,75];
y4 = [3,8,14,25,29,33];
y5 = [4,10,12,20,24,26];

% draw
hold on
plot(x, y1, 'r-', 'LineWidth', 2);
plot(x, y2, 'g-', 'LineWidth', 2);
plot(x, y3, 'b-', 'LineWidth', 2);
plot(x, y4, 'm-', 'LineWidth', 2);
plot(x, y5, 'c-', 'LineWidth', 2);
hold off


legend('(0,1)','[1,4)','[4,12)','[12,96)','>=96','Location','best')

xticks(x)
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'})
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('No. of anomalys','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);