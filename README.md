# Parallel-Programming

This project is refered to creating a **multi-threading** server in order to accept one or more clients requests to its DB, a lot faster. Requests can vary from searching or/and saving at server's DB, via clients (PUT-GET) keys and values. These are also structured/put in a circled-FIFO-queue so they can be processed.  All process requests are also timed. There was used UNIX's POSIX thread library.

***Notes***: 
 1. Run >**make all**
 2. Then run server: >**./server &**
 3. Start client: >**./client -a localhost -i 1 -p**
 4. then, to retrieve all saved data: >**./client -a localhost -i 1 -g**
 5. or to recall saved value for key (station.125): >**./client -a localhost -o GET:station.125**
 6. change at 5 the value of the key (station.125): >**./client -a localhost -o PUT:station.125**
 7. Have in mind to trace processes (**>ps**) if you need to **>kill** any. 
