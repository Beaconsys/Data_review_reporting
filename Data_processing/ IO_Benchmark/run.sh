#/bin/bash

mpicc Bench_read.c -o bread
mpicc Bench_write.c -o bwrite
Times=10

for((i=0;i<$Times;i++))
do
	#summit jobs to the TaihuLight
	sleep 10
done

