#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <assert.h>


//======== Data structure implementation =========

// Value structure
typedef struct node_value_t {
    char* value;
    struct node_value_t *next;
} node_value;

// linked list for seperate chaining
typedef struct node_t {
    char *key;
    node_value *value;
    struct node_t *next;
} node;

//hash table structure
typedef struct hash_table_t{
    int size; //size of table
    node **table; // Fixed size hash table
} hash_table;

// Global variable
hash_table **p;

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
    new_table->table = malloc(sizeof(node*) * size);
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
    for(; *str!='\0'; str++) {
	hashval = *str + (hashval << 5) - hashval;
    }

    //we return hash value mod hashtable size so it fits in to necessary range
    
    return hashval % hashtable->size;
    //return 1;
}

// Lookup the key and returns either the list of values or NULL
node* lookup(hash_table *hashtable, char *str){
    node *list;
    unsigned int hashval = hash(hashtable, str);

    //printf("hashval in lookup: %d\n", hashval);
    /* go to correct list based on hash value and see if str is in the list. If so, return a pointer to the list pointer */

    for(list = hashtable->table[hashval]; list != NULL; list = list->next) {
	
	//printf("list->key: %s\n", list->key);
	
	if(strcmp(str, list->key) == 0)
	    return list;
    }

    return NULL;
}


//inserting a string
int insert(hash_table *hashtable, char *str, char* val){
    
    node *new_list;
    node_value *new_value;
    //      list_t *current_list; //checking for duplicates
    unsigned int hashval = hash(hashtable, str);

    // Look for the key in hashtable. This will return the list of values
    node* key = lookup(hashtable, str);

    if (key == NULL) {
	
	//printf("--- Inside key not found\n");

	/*attempt to allocate memory for list*/
	if((new_list=malloc(sizeof(node)))==NULL) {
	    return 1; // error value
	}

	// insert into list
	// also need to insert the value here
	new_list->key = strdup(str);
	new_list->next = hashtable->table[hashval];
	hashtable->table[hashval] = new_list;
	
	
	// Allocate value for the value node
	if((new_value = malloc(sizeof(node_value))) == NULL) {
	    return 1; // error value
	}
	new_value->value = strdup(val);
	new_value->next = NULL;
	new_list->value = new_value;
    } else {
	//printf("--- Inside add value: %s\n", key->key);

	// Allocate value for the value node
	if((new_value = malloc(sizeof(node_value))) == NULL) {
	    return 1; // error value
	}
	new_value->value = strdup(val);
	new_value->next = key->value;
	key->value = new_value;
    
	
	//list_t_value *temp;
	//for(temp = key->value; temp != NULL; temp = temp->next) {
	//    printf("value: %d\n", temp->value);
	//}
	
    
    }
    return 0;
}

void test(hash_table *my_hash_table) {

    // ------------ Testing the insert after this -----------------------

    for (int i = 0; i < 5; i++) {
	if (insert(my_hash_table, "a", "1") == 0) {
	    //printf("Key successfully added\n");
	}
    }

    for (int i = 0; i < 5; i++) {
        if (insert(my_hash_table, "b", "2") == 0) {
            //printf("Key successfully added\n");
        }
    }
    
    //for (int i = 0; i < 2; i++) {
    //    if (insert(my_hash_table, "c", "3") == 0) {
    //        //printf("Key successfully added\n");
    //    }
    //}
    
    // ------------ Testing the lookup after this -----------------------

    node *key = lookup(my_hash_table, "a");

    if (key == NULL) {
	printf("key not found\n");
    } else {
	printf("key found: %s\n", key->key);
	node_value *temp;
	for(temp = key->value; temp != NULL; temp = temp->next) {
	    printf("value: %s\n", temp->value);
	}
    }


    key = lookup(my_hash_table, "b");

    if (key == NULL) {
	printf("key not found\n");
    } else {
	printf("key found: %s\n", key->key);
	node_value *temp;
	for(temp = key->value; temp != NULL; temp = temp->next) {
	    printf("value: %s\n", temp->value);
	}
    }

    //key = lookup(my_hash_table, "c");

    //if (key == NULL) {
    //    printf("key not found\n");
    //} else {
    //    printf("key found: %s\n", key->key);
    //    node_value *temp;
    //    for(temp = key->value; temp != NULL; temp = temp->next) {
    //        printf("value: %s\n", temp->value);
    //    }
    //}

}


void free_values(node_value* nv) {
   // printf("      ## free_values: addr of values: %p\n", nv);
    node_value* temp = nv;
    for (; temp != NULL; temp = nv) {
	nv = nv->next;
	//printf("      value: %s, addr: %p\n", temp->value, temp);
	free(temp->value);
	free(temp);
	//printf("      checking the value after freeing: %s\n", temp->value);
    }
    //printf("      ## free_values: Done freeing values at: %p\n", nv);
}



void free_node(node* n) {
    //printf("    #### free_node: addr of node: %p\n", n);
    
    // Going over all the values for the given bucket
    node* temp = n;
    for (; temp != NULL; temp = n) {
	n = n->next;
	//printf("    Freeing the values list for key: %s, addr: %p\n", temp->key, temp);	
	free_values(temp->value);
	free(temp->key);
	free(temp);
	//printf("    checking the value after freeing: %s\n", temp->key);
    }
    
    //printf("    #### free_node: Done freeing node at: %p\n", n);
}


void free_hash_table(hash_table *ht) {
    //printf("######## free_hash_table: addr of hash_table: %p\n", ht);
    
    // Going over all the elements in the hashtable
    for (int i = 0; i < ht->size; i++) {
	if (ht->table[i] != NULL) {
	    //printf("elem %d in table: %p. Not null\n", i, ht->table[i]);
	    node* temp = ht->table[i];

	    // Sending the node list (bucket) to be freed
	    //printf("key: %s, addr: %p\n", temp->key, temp);
	    free_node(temp);
	    //printf("checking the value after freeing: %s\n", temp->key);
	}
    }
    //printf("######## free_hash_table: Done free hash_table at: %p\n", ht);
    free(ht->table);
    free(ht);
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

    // Testing
    hash_table *my_hash_table;
    int size_of_table = 53;
    my_hash_table = create_hash_table(size_of_table);

    printf("Main: addr of hash_table: %p\n", my_hash_table);

    test(my_hash_table);
    free_hash_table(my_hash_table);

    
    /*
    p = malloc(sizeof(hash_table*) * num_reducers);
    
    if (p == 0) {
	printf("cannot allocate memory\n");
	return;
    }
    
    for (int i = 0; i < num_reducers; i++) {
	hash_table *temp_table;
	int size_of_table = 53;
	
	//printf("creating table\n");
	
	temp_table = create_hash_table(size_of_table);
	//printf("Done creating table\n");
	
	p[i] = temp_table;
	//printf("p[%d]: %p\n", i, p[i]);    
	//printf("Done adding table\n");
    }
    */

}
