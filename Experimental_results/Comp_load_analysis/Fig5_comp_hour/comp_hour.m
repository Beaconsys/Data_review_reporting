x = [47.1738616810203,
     47.169040249728234,
     47.191613645432623,
     46.84121995744942,
     46.56093489361922,
     46.07452324761284,
     45.56948757401945,
     45.81701728422106,
     45.78487844319104,
     46.0721347024996,
     46.53221818177849,
     46.43521561234086,
     46.543582184441734,
     46.881939104100523,
     47.15052371198789,
     47.586093028091286,
     47.5061967234464,
     47.13508793962964,
     46.948319583236714,
     47.05429386760184,
     47.126175701929074,
     47.49475151398773,
     47.430966711097533,
     47.22936958262668];

% Add extra data points at 0 and 23 with a small offset
xlabels = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'};

bar(x, 'Linewidth', 1);
h1 = legend('Compute node', 'Stripe reconfiguration');
set(h1, 'Orientation', 'horizon');
ylim([0 100]);

% Set x-axis labels and adjust their position
set(gca, 'XTick', 1:25);
set(gca, 'XTickLabel', xlabels);
set(gca, 'XAxisLocation', 'bottom');
set(gca, 'TickDir', 'out');
set(gca, 'TickLength', [0.005, 0.005]);
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Average node load (%)', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');
xlabel('Time (hour)', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');

% Set xlim to adjust the range and position of x-axis
xlim([0.5 24.5]);