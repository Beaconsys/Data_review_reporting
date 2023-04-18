#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define FILENAME "./data/data.txt"
#define BLOCK_SIZE (32 * 1)

int main(int argc, char *argv[]) {
  int rank, size;
  MPI_File fh;
  MPI_Offset offset;
  MPI_Status status;
  char buffer[BLOCK_SIZE];
  double start_time, end_time, elapsed_time;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  MPI_File_open(MPI_COMM_WORLD, FILENAME, MPI_MODE_CREATE | MPI_MODE_WRONLY,
                MPI_INFO_NULL, &fh);

  offset = rank * BLOCK_SIZE * 10;

  for (int i = 0; i < BLOCK_SIZE; i++) buffer[i] = 'a' + (rand() % 26);

  double sum_write = 0.1;
  int op_num = sum_write * 1024 * 1024 / BLOCK_SIZE;

  MPI_Barrier(MPI_COMM_WORLD);
  start_time = MPI_Wtime();

  for (int i = 0; i < op_num; i++)
    MPI_File_write_at(fh, offset, buffer, BLOCK_SIZE, MPI_CHAR, &status);

  MPI_Barrier(MPI_COMM_WORLD);

  end_time = MPI_Wtime();

  elapsed_time = end_time - start_time;
  double max_elapsed_time;
  MPI_Reduce(&elapsed_time, &max_elapsed_time, 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);

  if (rank == 0) {
    double iobw = (sum_write * size) / max_elapsed_time;
    printf("IOBW: %f MB\n", iobw);
  }

  MPI_File_close(&fh);
  MPI_Finalize();
  return 0;
}
