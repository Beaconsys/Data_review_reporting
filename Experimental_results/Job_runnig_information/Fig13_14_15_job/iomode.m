hold on all;
flag=1;
if flag==1
plot(transpose(iomode_c(1,:)),'r','linewidth',1);
plot(transpose(iomode_c(2,:)),'r--','linewidth',1);
plot(transpose(iomode_c(3,:)),'b','linewidth',1);
plot(transpose(iomode_c(4,:)),'b--','linewidth',1);
else
plot(transpose(iomode_v(1,:)),'r','linewidth',1);
plot(transpose(iomode_v(2,:)),'r--','linewidth',1);
plot(transpose(iomode_v(3,:)),'b','linewidth',1);
plot(transpose(iomode_v(4,:)),'b--','linewidth',1);
end;
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
ylim([0 1.1]);
legend('1-1','N-1','N-M','N-N');
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('I/O mode','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);