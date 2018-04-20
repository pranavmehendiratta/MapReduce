#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "mapreduce.h"
#include <assert.h>
#include <pthread.h>
#include <sys/types.h>

// ====== Type for the data structure ========
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
typedef struct hash_table_t {
    int size; //size of table
    node **table; // Fixed size hash table
    pthread_mutex_t **lock; 
} hash_table;

typedef struct proc_files_params_t {
    int numFiles; 
    Mapper map;
    pthread_mutex_t file_lock;
    char **files;
} proc_files;

// ======= Global variables ==========
hash_table **parts; // for holding the partitions
Partitioner partFunc; // Global variable for partition function
int numReducers;
int numMappers;
int filesProcessed;

// ====== Wrappers for pthread library ==========
#define hash_table_size 101
#define Pthread_mutex_lock(m)                                   assert(pthread_mutex_lock(m) == 0);
#define Pthread_mutex_unlock(m)                                 assert(pthread_mutex_unlock(m) == 0);
#define Pthread_create(thread, attr, start_routine, arg)        assert(pthread_create(thread, attr, start_routine, arg) == 0);
#define Pthread_join(thread, value_ptr)                         assert(pthread_join(thread, value_ptr) == 0);

//======== Data structure implementation =========

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

    // Allocating memory for table
    new_table->table = malloc(sizeof(node*) * size);
    if(new_table->table  ==  NULL) {
	return NULL;
    }

    // Allocating memory for the lock array
    new_table->lock = malloc(sizeof(pthread_mutex_t*) * size);
    if(new_table->lock  ==  NULL) {
	return NULL;
    }

    /*initialize the lements of the table*/
    for(int i = 0; i < size; i++)  {
	new_table->table[i] = NULL;
	new_table->lock[i] = malloc(sizeof(pthread_mutex_t)); // Allocating memory for the lock
	pthread_mutex_init(new_table->lock[i], NULL); // Initializing the lock
    }

    /*set the table's size*/
    new_table->size = size;
    return new_table;
}

// the hash function - Assuming to be correct
int hash(hash_table *hashtable, char *str) {

    unsigned int hashval;

    //hashing from 0
    hashval = 0;

    //for each char, we multiply old hash by 31 and add curr char
    for(; *str!='\0'; str++) {
	hashval = *str + (hashval << 5) - hashval;
    }

    //we return hash value mod hashtable size so it fits in to necessary range
    
    return hashval % hashtable->size;
}

/* Lookups the given key and return key of node type 
 * otherwise returns NULL
 */
node* lookup(hash_table *hashtable, char *str){
    node *list;
    unsigned int hashval = hash(hashtable, str);

    //printf("hashval in lookup: %d\n", hashval);
    
    // Linearly search through the bucket incase there is a collision for the 
    // current hashtable
    for(list = hashtable->table[hashval]; list != NULL; list = list->next) {
	
	//printf("list->key: %s\n", list->key);
	
	if(strcmp(str, list->key) == 0)
	    return list;
    }

    return NULL;
}

/* Inserts the given key in the given hashtable by creating
 * new objects of node and node_value type
 */
int insert(hash_table *hashtable, char *str, char* val){
    
    node *new_list;
    node_value *new_value;
    //      list_t *current_list; //checking for duplicates
    unsigned int hashval = hash(hashtable, str);

    // grabbing the lock for current bucket 
    pthread_mutex_lock(hashtable->lock[hashval]); 
    
    // Look for the key in hashtable. This will return the list of values
    node* key = lookup(hashtable, str);

    if (key == NULL) {
	
	//printf("--- Inside key not found\n");

	// Allocate memory for new node for the given key
	if((new_list = malloc(sizeof(node)))==NULL) {
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
    // Leaving the lock for current bucket 
    pthread_mutex_unlock(hashtable->lock[hashval]); 
    
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
    
    for (int i = 0; i < 2; i++) {
        if (insert(my_hash_table, "c", "3") == 0) {
            //printf("Key successfully added\n");
        }
    }
    
    // ------------ Testing the lookup after this -----------------------

    //node *key = lookup(my_hash_table, "a");

    //if (key == NULL) {
    //    printf("key not found\n");
    //} else {
    //    printf("key found: %s\n", key->key);
    //    node_value *temp;
    //    for(temp = key->value; temp != NULL; temp = temp->next) {
    //        printf("value: %s\n", temp->value);
    //    }
    //}


    //key = lookup(my_hash_table, "b");

    //if (key == NULL) {
    //    printf("key not found\n");
    //} else {
    //    printf("key found: %s\n", key->key);
    //    node_value *temp;
    //    for(temp = key->value; temp != NULL; temp = temp->next) {
    //        printf("value: %s\n", temp->value);
    //    }
    //}

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
	free(temp->value); // Freeing the strdup memory
	free(temp); // Freeing the node of the linkedlist
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
	free(temp->key); // Freeing the strdup memory
	free(temp); // Freeing hte node of the linkedlist
	//printf("    checking the value after freeing: %s\n", temp->key);
    }
    
    //printf("    #### free_node: Done freeing node at: %p\n", n);
}


void free_hash_table(hash_table *ht) {
    //printf("######## free_hash_table: addr of hash_table: %p\n", ht);
    
    // Going over all the elements in the hashtable
    for (int i = 0; i < ht->size; i++) {
	free(ht->lock[i]); // Free the lock for that bucket
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
    free(ht->lock);
    free(ht->table); // Freeing the memory for the hashtable - array
    free(ht); // Freeing the hashtable pointer
}

void free_partition(hash_table **p) {
    // TODO: This does not work perfectly,
    // free_hash_table is working correctly 

    // Freeing the memory allocated for hash tables
    for (int i = 0; i < numReducers; i++) {
	free_hash_table(p[i]);
	//free(p[i]);
    }
    free(p);
}

void dump_hash_table(hash_table* ht) {
    // Going over the buckets
    for (int i = 0; i < ht->size; i++) {
	printf("    ");
	
	// Checking if the bucket is not null
	if (ht->table[i] != NULL) {
	    printf("%d: \n", i);
	    node* temp = ht->table[i];

	    // Printing out the keys in that bucket
	    for (; temp != NULL; temp = temp->next) {
		printf("        ");
		printf("\"%s\" -> [", temp->key);
		node_value* temp1 = temp->value;

		// Printing out the values in for that key
		for (; temp1 != NULL; temp1 = temp1->next) {
		    printf(" \"%s\" ->", temp1->value);
		}
		printf(" NULL ]\n");
	    }
	} else {
	    printf("%d:", i);
	}
	
	printf("\n");
    }
}

void dump_hash_table_keys(hash_table* ht) {
    // Going over the buckets
    for (int i = 0; i < ht->size; i++) {
	
	// Checking if the bucket is not null
	if (ht->table[i] != NULL) {
	    node* temp = ht->table[i];

	    // Printing out the keys in that bucket
	    for (; temp != NULL; temp = temp->next) {
		printf("%s\n", temp->key);
		//node_value* temp1 = temp->value;
	    }
	} 
    }
}

void dump_partitions(hash_table** p) {
    for (int i = 0; i < numReducers; i++) {
        printf("Parition: %d -->\n", i);
	dump_hash_table(p[i]);
	//dump_hash_table_keys(p[i]);
    }
}

//======== Function to implement =================

void MR_Emit(char *key, char *value) {
    unsigned long partNum = (*partFunc)(key, numReducers);
    insert(parts[partNum], key, value);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
	hash = hash * 33 + c;
    return hash % num_partitions;
}

/* This will keep the thread busy as long as there 
 * are more files to process
 */
void process_files(proc_files* params) {
    
    int numFiles = params->numFiles;
    char **files = params->files;
    pthread_mutex_t file_lock = params->file_lock;
    Mapper map = params->map;

    while(1) {	

	int file_to_proc;
    
	// Find the file to process
	Pthread_mutex_lock(&file_lock);
	if (filesProcessed == numFiles) {
	    Pthread_mutex_unlock(&file_lock);
	    return;
	}
	file_to_proc = filesProcessed;	
	filesProcessed++;
	Pthread_mutex_unlock(&file_lock);
	
	printf("Thread: %lu, File being processed: %d->%s\n", pthread_self(), file_to_proc, files[file_to_proc]);
		
	// Process that file
	(*map)(files[file_to_proc]); // Calling the function from the thread
    }
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition) {

    // =========== Creating a single hash table for testing ========= 
    /*
    // Testing
    hash_table *my_hash_table;
    int size_of_table = 53;
    my_hash_table = create_hash_table(size_of_table);

    printf("Main: addr of hash_table: %p\n", my_hash_table);

    test(my_hash_table);
    dump_hash_table(my_hash_table);
    free_hash_table(my_hash_table);
    //*/

    // =========== Creating threads = # of files ====================
    /*
    char *files[10] = {"20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt", "20k.txt"};

    // Lock for giving a file to the mapper
    pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER; 

    // Setting the global variables
    partFunc = partition;
    numReducers = num_reducers;
    numMappers = num_mappers;

    // parts is a global variable for the partitions
    parts = malloc(sizeof(hash_table*) * numReducers);
    
    if (parts == 0) {
	printf("cannot allocate memory for partitions\n");
	return;
    }
    
    // Creating one hash table for each partition
    for (int i = 0; i < numReducers; i++) {
	hash_table *temp_table;
	temp_table = create_hash_table(hash_table_size);
	parts[i] = temp_table;
    }

    // # of mappers to create 
    int numthreads = 10; 
   
    printf("Input File: %s\n", argv[1]);

    // Variable for mappers
    pthread_t mappers_id[numthreads];

    // Create the # of mappers
    int file_proc = 0;
    for ( ; file_proc < numthreads; ) {
        Pthread_mutex_lock(&file_lock);
        Pthread_create(&mappers_id[file_proc], NULL, (void *)(*map), files[file_proc]);
        file_proc++;
        Pthread_mutex_unlock(&file_lock);
    }
   
    // Join all the mappers
    for (int i = 0; i < numthreads; i++) {
        Pthread_join(mappers_id[i], NULL);
    }

    dump_partitions(parts);
    
    // Free the partition created
    //printf("Starting to free the partitions...\n\n");
    free_partition(parts);
    //*/
    
    // =========== Any # of mappers can map any # of files ====================
    //*
    // Setting the global variables
    partFunc = partition;
    numReducers = num_reducers;
    numMappers = num_mappers;

    // parts is a global variable for the partitions
    parts = malloc(sizeof(hash_table*) * numReducers);
    
    if (parts == 0) {
	printf("cannot allocate memory for partitions\n");
	return;
    }
    
    // Creating one hash table for each partition
    for (int i = 0; i < numReducers; i++) {
	hash_table *temp_table;
	temp_table = create_hash_table(hash_table_size);
	parts[i] = temp_table;
    }

    // Initialize files processed
    filesProcessed = 0; // Set it to 1 when using arc and argv because the first value is the program name
    
    // TODO: Changing the function threads are calling
    char *files[10] = {"20k-1.txt", "20k.txt", "20k-1.txt", "20k.txt", "20k-1.txt", "20k.txt", "20k-1.txt", "20k.txt", "20k-1.txt", "20k.txt"};

    // Lock for giving a file to the mapper
    pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER; 
    
    // # of mappers to create 
    int numthreads = 5; 
    
    // Getting this from argc  
    int numFiles = 10; 
    
    // Setting the structure to send parameters to process_files
    proc_files params;
    params.numFiles = numFiles;
    params.files = files;
    params.map = map;
    params.file_lock = file_lock;
    
    // Variable for mappers
    pthread_t mappers_id[numthreads];
   
    // Function to call from each thread
    void (*pf)(proc_files*) = &process_files;

    // Calling process_files from threads
    for (int i = 0; i < numthreads; i++) {
        Pthread_create(&mappers_id[i], NULL, (void*)pf, (void*)&params);
    }
    
    // Join all the mappers
    for (int i = 0; i < numthreads; i++) {
        Pthread_join(mappers_id[i], NULL);
    }

    //dump_partitions(parts);

    free_partition(parts);
    //*/
}
