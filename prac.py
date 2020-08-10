import multiprocessing

def list_append(out_list, q):
    my_sum = sum(out_list)
    print("sum: ", my_sum)
    q.put(my_sum)

if __name__ == "__main__":
    lista_1 = [i for i in range(500)]# Number of random numbers to add
    lista_2 = [i for i in range(500,1000)]
    procs = 2   # Number of processes to create

    # Create a list of jobs and then iterate through
    # the number of processes appending each process to
    # the job list
    jobs = []
    q = multiprocessing.Queue()
    process_1 = multiprocessing.Process(target=list_append, args=(lista_1, q))
    process_2 = multiprocessing.Process(target=list_append, args=(lista_2, q))
    jobs.append(process_1)
    jobs.append(process_2)

    # Start the processes (i.e. calculate the random number lists)
    for j in jobs:
        print(j)
        j.start()

    total = q.get() + q.get()

    # Ensure all of the processes have finished
    for j in jobs:
        j.join()

    print(total)