import corrMatPlot.*
CMP=corrMatPlot(corr);
CMP=CMP.setColorMap(1);
CMP=CMP.setLabelStr({'Waiting\_time','Parallelism','No. of nodes','Submit\_Hour','Jobs\_submission','Jobs\_waiting'});
CMP=CMP.draw();
