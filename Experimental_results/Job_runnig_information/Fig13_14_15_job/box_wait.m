abn=zeros(1,6);
for i=1:1979348
    if wa(i,1)>1000
        wa(i,1)=NaN;
        abn(1)=abn(1)+1;
    end
    if wa(i,2)>1000
        wa(i,2)=NaN;
        abn(2)=abn(2)+1;
    end
    if wa(i,3)>1000
        wa(i,3)=NaN;
        abn(3)=abn(3)+1;
    end
    if wa(i,4)>1000
        wa(i,4)=NaN;
        abn(4)=abn(4)+1;
    end
    if wa(i,5)>1000
        wa(i,5)=NaN;
        abn(5)=abn(5)+1;
    end
    if wa(i,6)>1000
        wa(i,6)=NaN;
        abn(6)=abn(6)+1;
    end
end
h=boxplot(wa, 'Whisker',1.5);
set(h,'linewidth',1.5);
xtick=[1.5 3.5 5.5 7.5 9.5];
xtickname={'2017','2018','2019','2020','2021','2022'};
set(gca,'XTickLabel',xtickname,'Fontname', 'Arial','FontSize',20);
ylabel('Waiting time (s)','fontweight','bold','FontSize',25);
set(gca,'YMinorTick','on','YScale','log');
xlabel('Time (year)','fontweight','bold','FontSize',25);
ylim([0 1000]);