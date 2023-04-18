load('./total_job.mat');
count=[308202 223876 118040 1979348 139432 25677];
core=[mean(job2017(:,1)) mean(job2018(:,1)) mean(job2019(:,1)) mean(job2020(:,1)) mean(job2021(:,1)) mean(job2022(:,1))];
runtime=[mean(job2017(:,2)) mean(job2018(:,2)) mean(job2019(:,2)) mean(job2020(:,2)) mean(job2021(:,2)) mean(job2022(:,2))];
waittime=[mean(job2017(:,3)) mean(job2018(:,3)) mean(job2019(:,3)) mean(job2020(:,3)) mean(job2021(:,3)) mean(job2022(:,3))];
flag=5;
if flag==1
plot(count,'Linewidth',1);
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Number of jobs','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);
elseif flag==2
plot(core,'Linewidth',1);
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Average parallelism of jobs','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);
elseif flag==3
plot(runtime,'Linewidth',1);
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Average runtime of jobs','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);
elseif flag==4
plot(waittime,'Linewidth',1);
xticks([1:1:6]);
xticklabels({'2017', '2018', '2019', '2020', '2021', '2022'});
set(gca, 'Fontname', 'Arial', 'FontSize', 20);
ylabel('Average waitting time of jobs','fontweight','bold','FontSize',25);
xlabel('Time (year)','fontweight','bold','FontSize',25);
end