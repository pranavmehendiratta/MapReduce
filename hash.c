#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mapreduce.h"
#include <assert.h>

//////implementing hash map////////

//linked list for seperate chaining
typedef struct list_t_{
        char *string;
        int value;
        struct list_t_ *next;
}list_t;

//hash table structure
typedef struct _hash_table_t_{
        int size; //size of table
        list_t **table; //table of elements-dynamic array
}hash_table;


hash_table *create_hash_table(int size){
        hash_table *new_table;

        if(size<1)
                return NULL;
        /*allocating mem for the table structure*/
        if((new_table=malloc(sizeof(char)*100))==NULL)
                return NULL;
        /*allocatung memory for table*/
        if((new_table->table=malloc(sizeof(list_t*)*size))==NULL)
                return NULL;
        /*initialize the lements of the table*/
        for(int i=0;i<size;i++)
                new_table->table[i]=NULL;
        /*set the table's size*/
        new_table->size=size;
    
        return new_table;
}
    
//the hash function
int hash(hash_table *hashtable, char *str){
        unsigned int hashval;
        //hashing from 0
        hashval=0;
 //for each char, we multiply old hash by 31 and add curr char
        for(;*str!='\0';str++)
                hashval=*str+(hashval<<5)-hashval;
        //we return hash value mod hashtable size so it fits in to necessary range
        return hashval%hashtable->size;
}


/*searching for a particular string= hash string, do lsearch at array pos*/
list_t *lookup_string(hash_table *hashtable, char *str){
        list_t *list;
        unsigned int hashval=hash(hashtable, str);

        /* go to correct list based on hash value and see if str is in the list. If so, return a pointer to the list pointer */

        for(list=hashtable->table[hashval];list!=NULL; list=list->next){
                if(strcmp(str, list->string)==0)
                        return list;
        }

        return NULL;

}

//inserting a string
int add_key(hash_table *hashtable, char *str,int val){
        list_t *new_list;
        //      list_t *current_list; //checking for duplicates
        unsigned int hashval= hash(hashtable, str);

        /*attempt to allocate memory for list*/
        if((new_list=malloc(sizeof(list_t)))==NULL)
                return 1;

        //insert into list
        //also need to insert the value here
        new_list->string=strdup(str);
        new_list->value=val;
        new_list->next=hashtable->table[hashval];
        hashtable->table[hashval]=new_list;

        return 0;
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
        unsigned long hash=5381;
        int c;
        while((c=*key++)!='\0')
                hash= hash*33+c;

        return hash%num_partitions;
}


void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition){
        hash_table *my_hash_table;
        int size_of_table=12;
        my_hash_table=create_hash_table(size_of_table);

        add_key(my_hash_table, key, atoi(value));
}

void MR_Emit(char *key, char *value){
        //make copies of key-val pair, and when reduce is completed, free everything

}


int main(int argc, char** argv){
        hash_table *my_hash_table;
        int size_of_table=12;
        my_hash_table=create_hash_table(size_of_table);

        add_key(my_hash_table, "hello",10);
        //      hash(my_hash_table, "hello");

        //      create_hash_table(10);
        return 0;
}
