

  // __builtin_prefetch (&b[i+j], 0, 1);



/*****

*****/


/********
Read Prefetches
********/
void prefetchRead0(void * addr);
void prefetchRead0(void * addr){
    __builtin_prefetch (addr, 0, 0);
    }

void prefetchRead1(void * addr);
void prefetchRead1(void * addr){
    __builtin_prefetch (addr, 0, 1);
    }

void prefetchRead2(void * addr);
void prefetchRead2(void * addr){
    __builtin_prefetch (addr, 0, 2);
    }


void prefetchRead3(void * addr);
void prefetchRead3(void * addr){
    __builtin_prefetch (addr, 0, 3);
    }




/******
Write Prefetches
*******/
void prefetchWrite0(void * addr);
void prefetchWrite0(void * addr){
    __builtin_prefetch (addr, 1, 0);
    }
void prefetchWrite1(void * addr);
void prefetchWrite1(void * addr){
    __builtin_prefetch (addr, 1, 1);
    }

void prefetchWrite2(void * addr);
void prefetchWrite2(void * addr){
    __builtin_prefetch (addr, 1, 2);
    }    
void prefetchWrite3(void * addr);
void prefetchWrite3(void * addr){
    __builtin_prefetch (addr, 1, 3);
    }




