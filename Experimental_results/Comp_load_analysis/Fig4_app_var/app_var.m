load('./top_app.mat');
flag=1;
if flag==1
  data = [cesm_data; wrf_data;vasp_data;swcheck_data;grapes_data];
  data = data';
  boxplot(data);
  xtick=[1.5 3.5 5.5 7.5 9.5 11.5];
  xtickname={'CESM', 'WRF', 'VASP', 'SWCHECK', 'GRAPES'};
else
  data = [ww3_data; coawstm_data;lingo_data;nssolver_data;pmcl3d_data];
  data = data';
  boxplot(data);
  xtick=[1.5 3.5 5.5 7.5 9.5 11.5];
  xtickname={'WW3', 'COAWSTM', 'LINGO', 'NSSOLVER', 'PMCL3D'};  
end
ylim([0 1.1]);
set(gca,'XTickLabel',xtickname,'Fontname','Arial','FontSize',20);
ylabel('Normalization of I/O performance','fontweight','bold','FontSize',30);
xlabel('Applications','fontweight','bold','FontSize',30);

