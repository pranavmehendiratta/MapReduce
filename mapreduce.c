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
// for keeping sorted keys for each partition
typedef struct sort_t {
    char* key;
} sort;

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
    void *next_value;
} node;

//hash table structure
typedef struct hash_table_t {
    int size; //size of table
    node **table; // Fixed size hash table
    pthread_mutex_t **lock; 
    sort** uniq_keys;
    int num_keys;
} hash_table;

// Struct for sending parameters to mapper threads
typedef struct proc_files_params_t {
    int numFiles; 
    Mapper map;
    pthread_mutex_t file_lock;
    char **files;
} proc_files;

// Struct for sending parameters to reducer threads
typedef struct proc_data_structure_params_t {
    int parts_num; 
    Reducer reduce;
} proc_ds;

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

// ================ Sorting functions ===========

int comp_sort (const void *elem1, const void *elem2) 
{
    //printf("--- Inside string compare ---\n");
    sort* key1 = *(sort**)elem1;
    sort* key2 = *(sort**)elem2;
    //printf("key1: %s addr: %p, key2: %s addr: %p\n", key1->key, elem1, key2->key, elem2);
    //printf("--- Done with string compare ---\n");
    return strcmp(key1->key, key2->key);
}

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

    // Setting sort to NULL
    new_table->uniq_keys = NULL;

    // setting num_keys to zero
    new_table->num_keys = 0;

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

	// Updating next_value for get_next
	new_list->next_value = (void*)new_value;

	//if (strcmp(new_list->key, "please") == 0) {
	//    printf("key: %s, new_list->value: %p, new_list->next_value: %p\n", new_list->key, new_list->value, new_list->next_value);
	//}
	
	// Incrementing the count of keys in a hashtable
	hashtable->num_keys++; 

	// Allocating memory for keys
	hashtable->uniq_keys = realloc(hashtable->uniq_keys, sizeof(sort*) * hashtable->num_keys);

	// Adding key to the sort array
	hashtable->uniq_keys[hashtable->num_keys - 1] = malloc(sizeof(sort));
	hashtable->uniq_keys[hashtable->num_keys - 1]->key = strdup(str);
    } else {
	//printf("--- Inside add value: %s\n", key->key);

	//if (strcmp(key->key, "please") == 0) {
	//    printf("key: %s, new_list->value: %p, new_list->next_value: %p\n", key->key, key->value, key->next_value);
	//}

	// Allocate value for the value node
	if((new_value = malloc(sizeof(node_value))) == NULL) {
	    return 1; // error value
	}
	new_value->value = strdup(val);
	new_value->next = key->value;
	key->value = new_value;
	key->next_value = (void*)new_value; // Updating the next value for get_next

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

void free_array(sort** words, int size) {
    for (int i = 0; i < size; i++) {
	free(words[i]->key);
	free(words[i]);
    }
    free(words);
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

    for (int i = 0; i < ht->num_keys; i++) {
	free(ht->uniq_keys[i]->key);
	free(ht->uniq_keys[i]);
    }
    free(ht->uniq_keys);

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

char* get_next(char* key, int partition_num) {
    //printf("---- Inside get_next ----\n");

    node* n = lookup(parts[partition_num], key);
    //node_value* v = n->value;

    //for ( ; v!= NULL; v = v->next)	{
    //    printf("v: %p, value: %s\n", v, v->value);
    //}

    char* value_to_ret = NULL;
    if (n->next_value != NULL) {
	value_to_ret = ((node_value*)(n->next_value))->value;
    } else {
	return value_to_ret;
    }
    //printf("next_value is pointing to %p\n", n->next_value);
    //printf("value is pointing to %p\n", (void*)n->value);
    //printf("key: %s, next_value: %s\n", n->key, ((node_value*)(n->next_value))->value);
    node_value* temp = ((node_value*)(n->next_value));
    n->next_value = (void*)temp->next;

    //printf("n->next_value: %p\n", n->next_value);

    return value_to_ret;
}

void process_data_struct(proc_ds* params) {
    
    //printf("--- Inside process_data_struct ---\n");
    //printf("params->parts_num: %d\n", params->parts_num);
    
    Reducer reduce = params->reduce;
    qsort(parts[params->parts_num]->uniq_keys, parts[params->parts_num]->num_keys, sizeof(sort), comp_sort);

    for (int i = 0; i < parts[params->parts_num]->num_keys; i++) {
	//printf("i: %d, Key to reduce: %s\n", i, parts[params->parts_num]->uniq_keys[i]->key);
	(*reduce)(parts[params->parts_num]->uniq_keys[i]->key, get_next, params->parts_num);    
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
	parts[i] = create_hash_table(hash_table_size);
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

    // Starting reducing the data
    //char *key = "please";
    //for (int i = 0; i < 11; i++) {
    //    char* val = get_next(key, 9);
    //    printf("Got the value\n");
    //    if (val != NULL) {
    //        printf("%s\n", val);
    //    }
    //}

    // Checking the count of each partition
    //for (int i = 0; i < numReducers; i++) {
    //    qsort(parts[i]->uniq_keys, parts[i]->num_keys, sizeof(sort), comp_sort);
    //    printf("parts[%d]->num_key = %d\n", i, parts[i]->num_keys);
    //    for (int j = 0; j < parts[i]->num_keys; j++) {
    //        printf("%s\n", parts[i]->uniq_keys[j]->key);
    //    }
    //}
    
    // Sorting the keys
    //for (int i = 0; i < numReducers; i++) {
    //    qsort(parts[i]->uniq_keys, parts[i]->num_keys, sizeof(sort), comp_sort);
    //}
    
    // Starting reducing the data
    //char *key = "zur";
    //for (int i = 0; i < 11; i++) {
    //    char* val = get_next(key, 0);
    //    if (val != NULL) {
    //        printf("Got the value: %s\n", val);
    //    }
    //}

     
    //int red = 1;

    pthread_t reducers_id[numReducers];
    
    proc_ds params_reducers[numReducers];
    
    for (int i = 0; i < numReducers; i++) {
	params_reducers[i].parts_num = i;
	params_reducers[i].reduce = reduce;
    }

    void (*pds)(proc_ds*) = &process_data_struct;

    for (int i = 0; i < numReducers; i++) {
	Pthread_create(&reducers_id[i], NULL, (void*)pds, (void*)&params_reducers[i]);
    }

    // Join all the reducers
    for (int i = 0; i < numReducers; i++) {
	Pthread_join(reducers_id[i], NULL);
    }
    









    //*/
    free_partition(parts);
}
