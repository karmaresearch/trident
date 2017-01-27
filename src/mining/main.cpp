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


#include <mining/miner.h>

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>

#include <string>

namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

using namespace std;

bool initParams(int argc, const char** argv, po::variables_map &vm) {
    po::options_description cmdline_options("Parameters");
    cmdline_options.add_options()("input,i", po::value<string>(),
                                  "The path of the KB directory. This parameter is REQUIRED.")
        ("logLevel,l", po::value<logging::trivial::severity_level>(),
                                      "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is warning.")
        ("minSupport", po::value<int>(),
                                      "Minimum support for the patterns")
        ("minLength", po::value<int>(),
                                      "Minimum length of the patterns")
        ("maxLength", po::value<int>(),
                                      "Maximum length of the patterns");

    po::store(
        po::command_line_parser(argc, argv).options(cmdline_options).run(),
        vm);
    return true;
}

void initLogging(logging::trivial::severity_level level) {
    logging::add_common_attributes();
    logging::add_console_log(std::cerr,
                             logging::keywords::format =
                                 (logging::expressions::stream << "["
                                  << logging::expressions::attr <
                                  boost::log::attributes::current_thread_id::value_type > (
                                      "ThreadID") << " "
                                  << logging::expressions::format_date_time <
                                  boost::posix_time::ptime > ("TimeStamp",
                                          "%H:%M:%S") << " - "
                                  << logging::trivial::severity << "] "
                                  << logging::expressions::smessage));
    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= level);
}

int main(int argc, const char** argv) {

    //Init params
    po::variables_map vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }

    //Init logging system
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::warning;
    initLogging(level);

    //Get params
    string kbDir = vm["input"].as<string>();

    //Launch mining procedure
    Miner miner(kbDir, 1000000);
    miner.mine();
    BOOST_LOG_TRIVIAL(info) << "Get frequent patterns ... [" << vm["minLength"].as<int>() << "," << vm["maxLength"].as<int>() << "]";
    miner.getFrequentPatterns(vm["minLength"].as<int>(), vm["maxLength"].as<int>(), vm["minSupport"].as<int>());

    return 0;
}
