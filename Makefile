all: 
	gcc -o user user.c -Wall -Werror -pthread -O
clean: 
	rm -rf ./user
test: 
	./user input.1
memory: 
	valgrind --leak-check=full --show-leak-kinds=all -v ./user
