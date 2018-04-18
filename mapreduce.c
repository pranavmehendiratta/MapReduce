#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <assert.h>

//======== Data structure implementation =========

// linked list for seperate chaining
typedef struct list_t_{
    char *key;
    int value;
    struct list_t_ *next;
} list_t;

//hash table structure
typedef struct _hash_table_t_{
    int size; //size of table
    list_t **table; // Fixed size hash table
} hash_table;


// Intialize the hash table
hash_table *create_hash_table(int size) { 

    // New hash table
    hash_table *new_table;

    if(size<1)
	return NULL;

    // allocating mem for the table structure
    new_table = malloc(sizeof(hash_table));
    if(new_table == NULL) {
	return NULL;
    }

    /*allocatung memory for table*/
    new_table->table = malloc(sizeof(list_t*)*size);
    if(new_table->table  ==  NULL) {
	return NULL;
    }

    /*initialize the lements of the table*/
    for(int i = 0; i < size; i++)  {
	new_table->table[i] = NULL;
    }

    /*set the table's size*/
    new_table->size = size;

    return new_table;
}

// the hash function - Assuming to be correct
int hash(hash_table *hashtable, char *str) {

    unsigned int hashval;

    //hashing from 0
    hashval=0;

    //for each char, we multiply old hash by 31 and add curr char
    for(;*str!='\0';str++) {
	hashval=*str+(hashval<<5)-hashval;
    }

    //we return hash value mod hashtable size so it fits in to necessary range
    return hashval%hashtable->size;
}

// Lookup the key and returns either the list of values or NULL
int lookup_string(hash_table *hashtable, char *str){
    list_t *list;
    unsigned int hashval = hash(hashtable, str);

    //printf("hashval in lookup: %d\n", hashval);
    /* go to correct list based on hash value and see if str is in the list. If so, return a pointer to the list pointer */

    for(list = hashtable->table[hashval]; list != NULL; list = list->next) {
	if(strcmp(str, list->key) == 0)
	    return list->value;
    }

    return -1;
}


//inserting a string
int add_key(hash_table *hashtable, char *str,int val){
    
    list_t *new_list;
    //      list_t *current_list; //checking for duplicates
    unsigned int hashval = hash(hashtable, str);

    //printf("hashval in add_key: %d, value: %d\n", hashval, val);

    /*attempt to allocate memory for list*/
    if((new_list=malloc(sizeof(list_t)))==NULL)
	return 1;

    // insert into list
    // also need to insert the value here
    new_list->key = strdup(str);
    new_list->value = val;
    new_list->next = hashtable->table[hashval];
    hashtable->table[hashval] = new_list;

    return 0;
}




//======== Function to implement =================

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what you must define
void MR_Emit(char *key, char *value) {

}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
	hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, 
	Partitioner partition) {

    hash_table *my_hash_table;
    int size_of_table = 53;
    my_hash_table = create_hash_table(size_of_table);

    for (int i = 0; i < 10; i++) {
	add_key(my_hash_table, "hello", i * 10);
    }
    
    
    for (int i = 0; i < 10; i++) {
	printf("%d\n", lookup_string(my_hash_table, "hello"));
    }
    //      hash(my_hash_table, "hello");



}
