xlabels = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'};
% bar
bar(count, 'Linewidth', 1);
format long
% Set x-axis labels and adjust their position
set(gca, 'XTick', 1:25);
set(gca, 'XTickLabel', xlabels);
set(gca, 'XAxisLocation', 'bottom');
set(gca, 'TickDir', 'out');
set(gca, 'TickLength', [0.005, 0.005]);
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
xlabel('Time (hour)', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');
% Set xlim to adjust the range and position of x-axis
xlim([0.5 24.5]);

% add right Y-axis
yyaxis right;
plot(corhour_avg, 'r', 'LineWidth', 2.5, 'MarkerFaceColor', 'r');
ylabel('Average corehours','fontweight', 'bold','FontSize',25,'FontName', 'Arial')
% configure
yyaxis left;
set(gca, 'FontSize', 20, 'YColor', 'k');
set(0,'defaultAxesTickLabelInterpreter','tex');
ylabel('Number of jobs', 'fontweight', 'bold','FontSize',25,'FontName', 'Arial');
box on;

% add legend
legend('Number of jobs', 'Avg. core-hour consumption', 'Location', 'Northwest');
