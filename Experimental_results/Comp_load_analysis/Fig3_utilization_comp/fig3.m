x = 1:2102;
plot(x, y, 'LineWidth', 0.72);
xticks=[0 350 700 1050 1400 1750 2102];
yticks([0:20:100]);
xlim([0 2102]);
ylim([0 100]);
xtickname={'2017.4.1', '2018.3.15', '2019.2.28', '2020.2.13', '2021.1.29','2022.1.14','2022.12.31'};
legend('Compute node');
set(gca,'XTick',xticks,'XTickLabel',xtickname,'FontName', 'Arial','FontSize',20);
ylabel('Utilization rate (%)','fontweight','bold','FontSize',25,'FontName', 'Arial');
xlabel('Time','fontweight','bold','FontSize',25,'FontName', 'Arial');





