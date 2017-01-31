/*
 * Copyright 2017 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/


#include <trident/analytics/nativetasks.h>

//big endian
template<int D, int L>
int _native_cmp(const char *s1, const char *s2) {
    if (D > 0) {
        const long mask = ((long) 1 << (D * 8)) - 1;
        const unsigned long n1 = (*(unsigned long*)s1) & mask;
        if (n1) {
            return 1;
        }
        s1 += D;
    } else if (D < 0) {
        const long mask = ((long) 1 << (-D * 8)) - 1;
        const unsigned long n2 = (*(unsigned long*)s2) & mask;
        if (n2) {
            return -1;
        }
        s2 += -D;
    }
    for(int i = 0; i < L; ++i) {
        if (s1[i] != s2[i]) {
            return NativeTasks::sgn<int>(s1[i] - s2[i]);
        }
    }
    return 0;
}

template<int D, int L>
long _countSame(const char *p1,
        const char *e1,
        const int len1,
        const char *p2,
        const char *e2,
        const int len2) {
    long count = 0;
    while (p1 < e1 && p2 < e2) {
        const int cmp = _native_cmp<D, L>(p1, p2);
        if (cmp > 0) {
            p2 += len2 + 1;
        } else if (cmp < 0) {
            p1 += len1 + 1;
        } else {
            p1 += len1 + 1;
            p2 += len2 + 1;
            count++;
        }
    }
    return count;
}

long NativeTasks::GetCommon(const char *p1,
        const int len1,
        const long s1,
        const char *p2,
        const int len2,
        const long s2) {
    const char *e1 = p1 + s1 * (len1 + 1);
    const char *e2 = p2 + s2 * (len2 + 1);
    const int diff = len1 - len2;
    const int minL = min(len1, len2);
    switch (diff) {
        case -4:
            switch (minL) {
                case 1:
                    return _countSame<-4,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<-4,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<-4,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<-4,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<-4,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case -3:
            switch (minL) {
                case 1:
                    return _countSame<-3,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<-3,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<-3,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<-3,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<-3,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case -2:
            switch (minL) {
                case 1:
                    return _countSame<-2,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<-2,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<-2,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<-2,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<-2,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case -1:
            switch (minL) {
                case 1:
                    return _countSame<-1,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<-1,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<-1,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<-1,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<-1,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case 0:
            switch (minL) {
                case 1:
                    return _countSame<0,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<0,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<0,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<0,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<0,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case 1:
            switch (minL) {
                case 1:
                    return _countSame<1,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<1,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<1,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<1,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<1,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case 2:
            switch (minL) {
                case 1:
                    return _countSame<2,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<2,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<2,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<2,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<2,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case 3:
            switch (minL) {
                case 1:
                    return _countSame<3,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<3,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<3,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<3,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<3,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
        case 4:
            switch (minL) {
                case 1:
                    return _countSame<4,1>(p1, e1, len1, p2, e2, len2);
                case 2:
                    return _countSame<4,2>(p1, e1, len1, p2, e2, len2);
                case 3:
                    return _countSame<4,3>(p1, e1, len1, p2, e2, len2);
                case 4:
                    return _countSame<4,4>(p1, e1, len1, p2, e2, len2);
                case 5:
                    return _countSame<4,5>(p1, e1, len1, p2, e2, len2);
                default:
                    throw 10;
            }
    }
    throw 10;
}
