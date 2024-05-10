#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define FILENAME "./data/data.txt"
#define BLOCK_SIZE (1024 * 32)

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
  int ierr;
  char error_string[MPI_MAX_ERROR_STRING];
  int error_string_len;

  MPI_File_open(MPI_COMM_WORLD, FILENAME, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

  offset = rank * BLOCK_SIZE + 10;

  double sum_read = 16.0;
  int op_num = sum_read * 1024 * 1024 / BLOCK_SIZE;

  MPI_Barrier(MPI_COMM_WORLD);
  start_time = MPI_Wtime();

  for (int i = 0; i < op_num; i++)
    ierr = MPI_File_read_at(fh, offset, buffer, BLOCK_SIZE, MPI_CHAR, &status);

  MPI_Barrier(MPI_COMM_WORLD);

  end_time = MPI_Wtime();

  elapsed_time = end_time - start_time;
  double max_elapsed_time;
  MPI_Reduce(&elapsed_time, &max_elapsed_time, 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);

  if (rank == 0) {
    double iobw = (sum_read * size) / max_elapsed_time;
    printf("IOBW: %f MB\n", iobw);
  }

  MPI_File_close(&fh);
  MPI_Finalize();
  return 0;
}
