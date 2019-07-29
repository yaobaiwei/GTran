/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

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
    static bool trim_str(string& str);
};

#endif /* CLIENT_HPP_ */
