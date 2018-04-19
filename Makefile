all: 
	gcc -o user user.c -Wall -Werror -pthread -O
clean: 
	rm -rf ./user
test: 
	./user
memory: 
	valgrind --leak-check=full --show-leak-kinds=all -v ./user
