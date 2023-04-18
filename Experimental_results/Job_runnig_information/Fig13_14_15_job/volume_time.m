flag=1;
if flag==1
plot(vol/1024,'r', 'linewidth',1);
ylabel('Average I/O volume (GB)','fontweight','bold','FontSize',25);
legend('Jobs with non-trival I/O');
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
legend('I/O volume');%,'Parallelism');
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
xlabel('Time (year)','fontweight','bold','FontSize',25);
else
plot(iotime,'b', 'linewidth',1);
ylabel('Average I/O time (s)','fontweight','bold','FontSize',25);
legend('Jobs with non-trival I/O');
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
legend('I/O time');%,'Parallelism');
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
xlabel('Time (year)','fontweight','bold','FontSize',25);
end;
