/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
         Modified by Chenghuan Huang (chhuang@cse.cuhk.edu.hk)
*/
#include <vector>

#include "glog/logging.h"
#include "driver/client.hpp"

Client::Client(string cfg_fname) : fname_(cfg_fname) {
    id = -1;
}

void Client::Init() {
    nodes_ = ParseFile(fname_);
    CHECK(CheckUniquePort(nodes_));
    master_ = GetNodeById(nodes_, 0);
    cc_.Init(nodes_);
}

void Client::RequestWorker() {
    ibinstream m;
    obinstream um;
    m << id;
    cc_.Send(MASTER_RANK, m);
    cout << "[Client] Client just posted a REQ" << endl;

    // get the available worker ID
    cc_.Recv(MASTER_RANK, um);
    um >> id;
    um >> handler_;

    cout << "[Client] Client " << id << " recvs a REP: get available worker_node" << handler_ - 1 << endl << endl;
}

string Client::CommitQuery(string query) {
    ibinstream m;
    obinstream um;

    char hostname[HOST_NAME_MAX];
    gethostname(hostname, HOST_NAME_MAX);
    string host_str(hostname);
    m << host_str;
    m << query;

    cc_.Send(handler_, m);
    cout << "[Client] Client posts the query to worker_node" << handler_ - 1 << endl << endl;

    cc_.Recv(handler_, um);

    string result;
    string hname;
    uint64_t time_;
    vector<value_t> values;
    um >> hname;
    um >> values;
    um >> time_;

    result = "";
    if (values.size() == 0) {
        result = "Query '" + query + "' result: \n";
        result += "=>Empty\n";
    } else {
        result = "Query '" + query + "' result: \n";
        for (auto& v : values) {
            result += "=>" + v.DebugString() + "\n";
        }
    }

    result += "[Timer] ";
    if (time_ / 1000 == 0) {
        result += to_string(time_) + " us for ProcessQuery";
    } else {
        stringstream ss;
        ss << std::fixed << std::setprecision(2) << (time_ / 1000.0);
        result += ss.str() + " ms for ProcessQuery";
    }


    return result;
}

void Client::run_query(string query, string& result, bool isBatch) {
    cout << endl;
    cout << "[Client] Processing query : " << query << endl;

    RequestWorker();

    if (isBatch) {
        result += CommitQuery(query) + "\n";
    } else {
        result = CommitQuery(query);
    }
}

void Client::print_help() {
    cout << endl;
    cout << "GQuery commands: " << endl;
    cout << "    help                display general help infomation" << endl;
    cout << "    help index          display help infomation for building index" << endl;
    cout << "    help config         display help infomation for setting config" << endl;
    cout << "    help emu            display help infomation for running emulation of througput test" << endl;
    cout << "    help status         display help infomation for displaying system status" << endl;
    cout << "    quit                quit from console" << endl;
    cout << "    gquery <args>       run Gremlin-Like queries" << endl;
    cout << "        -q <query> [<args>] a single query input by user" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -f <file> [<args>]  a single query from <file>" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -b <file> [<args>]  a set of queries configured by <file> (batch-mode)" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -t <file> [<args>]  a set of queries configured by <file> (transaction-mode)" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << endl;
}

void Client::print_build_index_help() {
    cout << endl;
    cout << "Help information for building index:" << endl;
    cout << endl;
    cout << "Usage:" << endl;
    cout << "    BuildIndex(<V/E>,'<property_key>')" << endl;
    cout << endl;
    cout << "Example:" << endl;
    cout << "    gquery -q BuildIndex(V,'name')" << endl;
    cout << endl;
}

void Client::print_set_config_help() {
    cout << endl;
    cout << "Help information for setting config:" << endl;
    cout << endl;
    cout << "Usage:" << endl;
    cout << "    SetConfig(<option_name>,<option_value>)" << endl;
    cout << endl;
    cout << "Option value types:" << endl;
    cout << "    boolean : 'y', 't', 'true' for true; 'n', 'f', 'false' for false" << endl;
    cout << "    int : An integer value" << endl;
    cout << "    isolation_level: 'SERIALIZABLE' or 'SNAPSHOT'" << endl;
    cout << endl;
    cout << "Available options:" << endl;
    cout << "    caching: boolean" << endl;
    cout << "    core_bind: boolean" << endl;
    cout << "    expert_division: boolean" << endl;
    cout << "    step_reorder: boolean" << endl;
    cout << "    indexing: boolean" << endl;
    cout << "    stealing: boolean" << endl;
    cout << "    data_size: int" << endl;
    cout << "    opt_preread: boolean" << endl;
    cout << "    opt_validation: boolean" << endl;
    cout << "    iso_level (Not Supported Yet): isolation_level" << endl;
    cout << "    abort_rerun_times: int" << endl;
    cout << endl;
    cout << "Example:" << endl;
    cout << "    gquery -q SetConfig(expert_division,f)" << endl;
    cout << endl;
}

void Client::print_run_emu_help() {
    cout << endl;
    cout << "Help information for running emulator mode:" << endl;
    cout << endl;
    cout << "Usage:" << endl;
    cout << "    emu <emu_config_file> <emu_query_file>" << endl;
    cout << endl;
    cout << "The config file contains 2 lines:" << endl;
    cout << "1   <seconds_of_emulation> <parallel_factor>" << endl;
    cout << "2   <read_ratio> <write_ratio>" << endl;
    cout << endl;
    cout << "The format of the query file:" << endl;
    cout << "    [INSERT]" << endl;
    cout << "    <lines of INSERT queries ...>" << endl;
    cout << "    [READ]" << endl;
    cout << "    <lines of READ queries ...>" << endl;
    cout << "    [UPDATE]" << endl;
    cout << "    <lines of UPDATE queries ...>" << endl;
    cout << "    [DROP]" << endl;
    cout << "    <lines of DROP queries ...>" << endl;
    cout << "    [MIX]" << endl;
    cout << "    <lines of MIX queries ...>" << endl;
    cout << "This is an example of a query line:" << endl;
    cout << "    g.V().has(\"firstName\",\"$RAND\");g.E().has(\"creationDate\",\"$RAND\") V firstName E creationDate" << endl;
    cout << endl;
}

void Client::print_display_status_help() {
    cout << endl;
    cout << "Help information for displaying system status:" << endl;
    cout << "Usage:" << endl;
    cout << "    DisplayStatus(<status_key>)" << endl;
    cout << endl;
    cout << "Available status keys:" << endl;
    cout << "    mem: Display memory info of containers " << endl;
    cout << "    gc: Display dependent gc tasks' status " << endl;
    cout << endl;
    cout << "Example:" << endl;
    cout << "    gquery -q DisplayStatus(mem)" << endl;
    cout << endl;
}

bool Client::trim_str(string& str) {
    size_t pos = str.find_first_not_of(" \t");  // trim blanks from head
    if (pos == string::npos) return false;
    str.erase(0, pos);

    pos = str.find_last_not_of(" \t");  // trim blanks from tail
    str.erase(pos + 1, str.length() - (pos + 1));

    return true;
}

void Client::run_console(string query_fname) {
    if (query_fname != "") {
        string query, result;

        ifstream file(query_fname.c_str());
        if (!file) {
          cout << "[Client][ERROR]: " << query_fname << " does not exist." << endl << endl;
          return;
        }

        while (std::getline(file, query)) {
            if (!trim_str(query)) {
              cout << "[Client][Error]: Empty Query" << endl << endl;
              return;
            }

            run_query(query, result, true);
            query.clear();
        }

        cout << "[Client] result: " << result << endl << endl;
        return;
    }

    ConsoleUtil* console = ConsoleUtil::GetInstance();
    console->SetConsoleHistory("history_gquery.log");
    console->SetOnQuitWrite("history_gquery.log");

    while (true) {
    next:
        string cmd = console->TryConsoleInput("GQuery> ");

        // Timer
        uint64_t enter_t = timer::get_usec();

        // trim input and check input is empty
        if (!trim_str(cmd)) {
            goto next;
        }

        if (cmd == "help index") {
            print_build_index_help();
            continue;
        }

        if (cmd == "help config") {
            print_set_config_help();
            continue;
        }

        if (cmd == "help emu") {
            print_run_emu_help();
            continue;
        }

        if (cmd == "help status") {
            print_display_status_help();
            continue;
        }

        // General usage
        if (cmd == "help" || cmd == "h") {
          print_help();
          continue;
        }

        // run cmd
        if (cmd == "quit" || cmd == "q") {
          exit(0);
        } else {
          std::stringstream cmd_ss(cmd);
          std::string token;

          cmd_ss >> token;
          if (token == "gquery") {
            string query, result;
            string fname, bname, ofname, tname;
            bool s_enable = false, f_enable = false, b_enable = false, o_enable = false, t_enable = false;

            // get parameters
            while (cmd_ss >> token) {
              if (token == "-q") {
                // single query by command
                std::getline(cmd_ss, query);
                if (!trim_str(query)) {
                    cout << "[Client][Error]: Empty Query" << endl << endl;
                    goto next;
                }
                s_enable = true;
              } else if (token == "-f") {
                // single query in file
                cmd_ss >> fname;
                f_enable = true;
              } else if (token == "-b") {
                // set of queries
                cmd_ss >> bname;
                b_enable = true;
              } else if (token == "-o") {
                // output to file
                cmd_ss >> ofname;
                o_enable = true;
              } else if (token == "-t") {
                // transaction Mode
                cmd_ss >> tname;
                t_enable = true;
              } else {
                goto failed;
              }
            }  // gquery_while

            if (!s_enable && !f_enable && !b_enable && !t_enable) goto failed;  // meaningless

            if (s_enable) {  // -s <query>
                run_query(query, result, false);

                if (o_enable) {
                  std::ofstream ofs(ofname, std::ofstream::out);
                  ofs << result;
                } else {
                  cout << "[Client] result: " << result << endl << endl;
                }
            }

            if (f_enable) {  // -f <file>
              cout << "[Client] Single query in file" << endl;

              ifstream file(fname.c_str());
              if (!file) {
                  cout << "[Client][ERROR]: " << fname << " does not exist." << endl << endl;
                  goto next;
              }

              // read only one line
              std::getline(file, query);
              if (!trim_str(query)) {
                  cout << "[Client][Error]: Empty Query" << endl << endl;
                  goto next;
              }

              run_query(query, result, false);

              if (o_enable) {
                std::ofstream ofs(ofname, std::ofstream::out);
                ofs << result;
              } else {
                cout << "[Client] result: " << result << endl << endl;
              }
            }

            if (b_enable) {  // -b <file>
              cout << "[Client] b_enable" << endl;

              ifstream file(bname.c_str());
              if (!file) {
                  cout << "[Client][ERROR]: " << bname << " does not exist." << endl << endl;
                  goto next;
              }

              while (std::getline(file, query)) {
                  if (!trim_str(query)) {
                      cout << "[Client][Error]: Empty Query" << endl << endl;
                      goto next;
                  }

                  run_query(query, result, true);

                  query.clear();
              }

              if (o_enable) {
                std::ofstream ofs(ofname, std::ofstream::out);
                ofs << result;
              } else {
                cout << "[Client] result: " << result << endl << endl;
              }
            }

            if (t_enable) {
              cout << "[Client] Transaction mode" << endl;
              ifstream file(tname.c_str());
              if (!file) {
                cout << "[Client][ERROR]: " << bname << " does not exist." << endl << endl;
                goto next;
              }

              string trx_cmd;
              while (std::getline(file, query)) {
                if (!trim_str(query)) {
                  cout << "[Client][Error]: Empty Query" << endl << endl;
                  goto next;
                }
                trx_cmd += query + "\n";
                query.clear();
              }
              if (trx_cmd == "") {
                cout << "[Client][Error]: Empty Transaction" << endl << endl;
                goto next;
              }

              run_query(trx_cmd, result, true);

              if (o_enable) {
                std::ofstream ofs(ofname, std::ofstream::out);
                ofs << result;
              } else {
                cout << "[Client] result: " << result << endl << endl;
              }
            }
          } else {  // if token != "gquery"
        failed:
            cout << "[Client][ERROR]: Failed to run the command: " << cmd << endl << endl;
            print_help();
          }
        }

        uint64_t end_t = timer::get_usec();
        cout << "[Timer] " << (end_t - enter_t) / 1000 << " ms for whole process." << endl;
    }
}


int main(int argc, char* argv[]) {
    if (argc != 2 && argc != 3) {
        cout << "1 or 2 params required" << endl;
        return 0;
    }

    google::InitGoogleLogging(argv[0]);
    string cfg_fname = argv[1];
    CHECK(!cfg_fname.empty());

    string query_fname;
    if (argc == 2) {
        query_fname = "";
    } else {
        query_fname = argv[2];
    }

    Client client(cfg_fname);
    client.Init();
    cout << "DONE -> Client->Init()" << endl;

    client.run_console(query_fname);
    return 0;
}
