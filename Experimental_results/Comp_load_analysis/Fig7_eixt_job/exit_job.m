x = [44476, 31827, 24615, 20251, 19435, 18799, 19868, 21885, 36064, 71924, 105233, 82762, 58568, 71685, 104378, 126772, 124394, 93974, 66809, 71155, 83209, 81417, 68431, 57086];

% Add extra data points at 0 and 23 with a small offset
xlabels = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'};

bar(x, 'Linewidth', 1);
h1 = legend('Abnormally terminated job', 'Stripe reconfiguration');
set(h1, 'Orientation', 'horizon');
ylim([0 130000]);

% Set x-axis labels and adjust their position
set(gca, 'XTick', 1:25);
set(gca, 'XTickLabel', xlabels);
set(gca, 'XAxisLocation', 'bottom');
set(gca, 'TickDir', 'out');
set(gca, 'TickLength', [0.005, 0.005]);
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Number of jobs', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');
xlabel('Time (hour)', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');

% Set xlim to adjust the range and position of x-axis
xlim([0.5 24.5]);