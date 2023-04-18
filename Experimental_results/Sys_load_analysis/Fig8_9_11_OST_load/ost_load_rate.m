x = [3.891915093175366,
 3.519232524109238,
 3.1012375663334586,
 2.945090178240863,
 2.787315738439026,
 2.7601574020204143,
 2.911276387933922,
 2.7181356464096167,
 2.9330788949419073,
 3.8999040214382674,
 4.266557712311139,
 4.280414026551008,
 4.097319229209641,
 4.6834385148357764,
 5.282966449815768,
 5.969569083760868,
 6.005714074152434,
 5.0442432256130845,
 4.548891310096792,
 4.529254138678798,
 4.591581248567551,
 5.208363745350046,
 4.622792757532484,
 4.226364626857777]

% Add extra data points at 0 and 23 with a small offset
xlabels = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23'};

bar(x, 'Linewidth', 1);
h1 = legend('OST', 'Stripe reconfiguration');
set(h1, 'Orientation', 'horizon');
ylim([0 8]);

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