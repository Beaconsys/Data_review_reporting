labels_corehour={'PIO 51.3%', 'MPI-IO 7.4%','NetCDF 22.7%','HDF5 1.1%','POSIX 17.5%'};
labels_volume={'PIO 22.6%', 'MPI-IO 16.5%','NetCDF 54.8%','HDF5 2.3%','POSIX 3.8%'};
flag=1;
if flag==1
pie(lib(:,1),labels_corehour);
else
pie(lib(:,2),labels_volume);
end;
set(gca, 'Fontname', 'Arial', 'FontSize', 20);