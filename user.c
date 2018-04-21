#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "mapreduce.c"

void Map(char *file_name) {
    //printf("file_name in Map function: %s\n", file_name);
    
    FILE *fp = fopen(file_name, "r");
    assert(fp != NULL);

    char *line = NULL;
    size_t size = 0;
    
    
    //printf("Calling MR_Emit...\n");
    
    while (getline(&line, &size, fp) != -1) {
        char *token, *dummy = line;
        while ((token = strsep(&dummy, " \t\n\r")) != NULL) {
	    MR_Emit(token, "1");
        }
    }

    //printf("Done with Map function...\n");

    fclose(fp);
}


void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL)
	count++;
    
    printf("%s %d\n", key, count);
}


int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
}
