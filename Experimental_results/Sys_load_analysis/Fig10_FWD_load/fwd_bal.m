xline=100;
yrow=1008;
table=zeros(xline,yrow);
for i=1:xline
   for j=1:yrow
    tmp=fwdbal(i,j);
    if tmp<= 0.1
        table(i,j)=1;
    elseif tmp <= 0.3
        table(i,j)=2;
    elseif tmp<= 0.6
        table(i,j)=3;
    else
        table(i,j)=4;
    end;
  end;
end;
imagesc(table);
color_map = [1 1 1;0.7 0.7 0.7;0.3 0.3 0.3;0 0 0];
colormap(color_map);
color_bar_xtick_bar = {'Idle', 'Low', 'Mid', 'High'};
color_bar_ticks =[1.4 2.15 2.9 3.65];
colorbar('FontSize',20,'Xtick',color_bar_ticks,'XTickLabel',color_bar_xtick_bar);
xticks(0:168:yrow);
xticklabels(xticks);
limit = [0 yrow+1];
for i = 1 : xline
        line(limit, [i+0.5 i+0.5], 'color', 'k');
end

for i = 1 : yrow
        line([i+0.5 i+0.5], [0 xline+1], 'color', 'k');
end
set(gca,'Fontname','Arial','FontSize',30,'YTickLabel',{});
ylabel('I/O forwarding node','Fontweight','bold','FontSize',40);
xlabel('Time (hour)','Fontweight','bold','FontSize',40);
