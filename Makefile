make:
	gcc -o main_server main_server.c 
	gcc -o main_client main_client.c
clean:
	rm main_client main_server
