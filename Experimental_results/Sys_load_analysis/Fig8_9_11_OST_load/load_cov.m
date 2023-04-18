plot(BF,'r','Linewidth',1);
hold on;
plot(AF,'b','Linewidth',1);
legend('Before July 2020','After July 2020');
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Coefficient of variation (COV) of OST load', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');
xlabel('Time (hour)', 'fontweight', 'bold', 'FontSize', 25, 'Fontname', 'Arial');
xlim([0,2200]);
ylim([0 15]);
