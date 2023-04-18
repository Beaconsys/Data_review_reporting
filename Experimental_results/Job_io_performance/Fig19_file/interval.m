xlabels={'(0-1)', '[1-2)','[2-4)', '[4-8)', '[8-16)', '[16-32)', '[32,¡Þ)'};
bar(1:1:7, interv);
xticks(1:1:7);
xticklabels(xlabels);
xtickangle(45);
set(gca,'Fontname', 'Arial', 'FontSize', 20);
ylabel('File access times','fontweight','bold','FontSize',25);
set(gca,'YMinorTick','on','YScale','log');
xlabel('File access interval','fontweight','bold','FontSize',25);