all: 
	gcc -o user user.c -Wall -Werror -pthread -O
clean: 
	rm -rf ./user

