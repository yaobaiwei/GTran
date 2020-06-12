// Copyright 2019 BigGraph Team @ Husky Data Lab, CUHK
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CLIENT_HPP_
#define CLIENT_HPP_

#include <unistd.h>
#include <limits.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <set>
#include <vector>

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "base/type.hpp"
#include "base/serialization.hpp"
#include "base/client_connection.hpp"
#include "utils/global.hpp"
#include "utils/timer.hpp"
#include "core/message.hpp"

#include "glog/logging.h"

#include "utils/console_util.hpp"

class Client{
 public:
    explicit Client(string cfg_fname);

    void Init();

    void run_console(string query_fname);

 private:
    int id;
    string fname_;
    vector<Node> nodes_;
    Node master_;
    ClientConnection cc_;
    int handler_;

    void RequestWorker();
    string CommitQuery(string query);

    void run_query(string query, string& result, bool isBatch);

    static void print_help();
    static void print_build_index_help();
    static void print_set_config_help();
    static void print_run_emu_help();
    static void print_display_status_help();
    static bool trim_str(string& str);
};

#endif /* CLIENT_HPP_ */
