//
//  test_lru.cpp
//  trident
//
//  Created by Jacopo Urbani on 15/06/14.
//  Copyright (c) 2014 Jacopo Urbani. All rights reserved.
//

#include "../src/utils/hashmap.h"
#include "../src/utils/utils.h"

#include <iostream>

using namespace std;

char s[512];

void copy_string(char *output, const char* input) {
    Utils::encode_short(output, 0, strlen(input));
    memcpy(output+2,input, strlen(input));
}

int main(int argc, const char** args) {
    LRUByteArraySet set(5,100);
    
    //Add "a"
    cout << "Adding 'a'" << endl;
    copy_string(s, "a");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'b'" << endl;
    copy_string(s, "b");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'c'" << endl;
    copy_string(s, "c");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'd'" << endl;
    copy_string(s, "d");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'e'" << endl;
    copy_string(s, "e");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'd'" << endl;
    copy_string(s, "d");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'b'" << endl;
    copy_string(s, "b");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'b'" << endl;
    copy_string(s, "b");
    set.put(s);
    cout << set.toString() << endl;
    
    cout << "Adding 'e'" << endl;
    copy_string(s, "e");
    set.put(s);
    cout << set.toString() << endl;
}