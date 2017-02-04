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


#ifndef _MINER_H
#define _MINER_H

#include <trident/kb/kb.h>
#include <trident/kb/dictmgmt.h>
#include <kognac/fpgrowth.h>

#include <string>
#include <vector>

using namespace std;

typedef std::pair<uint32_t, uint32_t> PatternElement;

class MyPatternContainer : public PatternContainer<PatternElement> {
	private:
		DictMgmt *dict;
		std::vector<FPattern<PatternElement>> patterns;

	public:
		MyPatternContainer(int minLen, DictMgmt *dict) :
			PatternContainer(minLen),
			dict(dict) {
			}

		string vector2string(const std::vector<PatternElement>&);

		void add(const FPattern<PatternElement> &);

		void printOrderedBySupport();
};

class Miner {
	private:
		std::unique_ptr<KB> kb;
		DictMgmt *dict;

		PatternElement rootValue;
		FPTree<PatternElement> tree;

		void processPattern(std::vector<PatternElement>&);

	public:
		Miner(string kbDir, const int fptreeInternalBufferSize);

		void mine();

		void getFrequentPatterns(const int minLen, const int maxLen, const int minSupport);

};

#endif
