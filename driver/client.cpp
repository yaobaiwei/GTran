/*
 * client.cpp
 *
 *  Created on: Jun 24, 2018
 *      Author: Hongzhi Chen
 */

#include "driver/client.hpp"

Client::Client(string cfg_fname) : fname_(cfg_fname) {
    id = -1;
}

void Client::Init(){
    nodes_ = ParseFile(fname_);
    CHECK(CheckUniquePort(nodes_));
    master_ = GetNodeById(nodes_, 0);
    cc_.Init(nodes_);
}

void Client::RequestWorker(){
    ibinstream m;
    obinstream um;
    m << id;
    cc_.Send(MASTER_RANK, m);
    cout << "[Client] Client just posted a REQ" << endl;

    //get the available worker ID
    cc_.Recv(MASTER_RANK, um);
    um >> id;
    um >> handler;
    cout << "[Client] Client " << id << " recvs a REP: get available worker" << handler << endl << endl;
}

string Client::CommitQuery(string query){
    ibinstream m;
    obinstream um;

    char hostname[HOST_NAME_MAX];
    gethostname(hostname, HOST_NAME_MAX);
    string host_str(hostname);
    m << host_str;
    m << query;
    cc_.Send(handler, m);
    cout << "[Client] Client posts the query to worker" << handler << endl << endl;

    cc_.Recv(handler, um);

    string result;
	string hname;
    vector<value_t> values;
    um >> hname;
    um >> values;

	result = "";
	if (values.size() == 0) {
    	result = "Query '" + query + "' result: \n";
		result += "=>Empty\n";
	} else {
		result = "Query '" + query + "' result: \n";
		for(auto& v : values){
			result += "=>" + Tool::DebugString(v) + "\n";
		}
	}

    return result;
}

void Client::run_query(string query, string& result, bool isBatch) {
    cout << endl;
    cout << "[Client] Processing query : " << query << endl;

    RequestWorker();

    uint64_t touchWorker_t = timer::get_usec();
    if (isBatch) {
        result += CommitQuery(query) + "\n";
    } else {
        result = CommitQuery(query);
    }
    uint64_t endWorker_t = timer::get_usec();
    if ((endWorker_t - touchWorker_t) / 1000 == 0)
        cout << "[Timer] " << (endWorker_t - touchWorker_t) << " us for CommitQuery" << endl;
    else
        cout << "[Timer] " << (endWorker_t - touchWorker_t) / 1000 << " ms for CommitQuery" << endl;
}

void Client::print_help()
{
    cout << "GQuery commands: " << endl;
    cout << "    help                display help infomation" << endl;
    cout << "    quit                quit from console" << endl;
    // cout << "    config <args>       run commands on config" << endl;
    // cout << "        -v                  print current config" << endl;
    // cout << "        -l <file>           load config items from <file>" << endl;
    // cout << "        -s <string>         set config items by <str> (format: item1=val1&item2=...)" << endl;
    cout << "    gquery <args>       run Gremlin-Like queries" << endl;
    cout << "        -q <query> [<args>] a single query input by user" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -f <file> [<args>]  a single query from <file>" << endl;
    // cout << "           -n <num>            run <num> times" << endl;
    // cout << "           -v <num>            print at most <num> lines of results" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -b <file> [<args>]  a set of queries configured by <file> (batch-mode)" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
}

bool Client::trim_str(string& str) {
    size_t pos = str.find_first_not_of(" \t"); // trim blanks from head
    if (pos == string::npos) return false;
    str.erase(0, pos);

    pos = str.find_last_not_of(" \t");  // trim blanks from tail
    str.erase(pos + 1, str.length() - (pos + 1));

    return true;
}

void Client::run_console() {
    
  // Timer:
  // 1. Whole time for user
  // 2. Touch Worker to Get result 
  while (true) {
next:
    string cmd;
    cout << "GQuery> ";
    std::getline(std::cin, cmd);

    // Timer
    uint64_t enter_t = timer::get_usec();

    // trim input and check input is empty
    if (!trim_str(cmd)) {
        goto next;
    }

    // Usage
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
        string fname, bname, ofname;
        bool s_enable = false, f_enable = false, b_enable = false, o_enable = false;

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
          } else {
            goto failed;
          }
        } // gquery_while

        if (!s_enable && !f_enable && !b_enable) goto failed; // meaningless

        if (s_enable) { // -s <query>
            run_query(query, result, false);

            if (o_enable) {
              std::ofstream ofs(ofname, std::ofstream::out);
              ofs << result;
            } else {
              cout << "[Client] result: " << result << endl << endl;
            }
        }

        if (f_enable) { // -f <file>
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

        if (b_enable) { // -b <file>
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

      } else {
failed:
        cout << "[Client][ERROR]: Failed to run the command: " << cmd << endl << endl;
        print_help();
      }

    }

    uint64_t end_t = timer::get_usec();
    cout << "[Timer] " << (end_t - enter_t) / 1000 << " ms for whole process." << endl;
  }
}

//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
    if(argc != 2){
        cout << "1 params required" <<endl;
        return 0;
    }
    google::InitGoogleLogging(argv[0]);
    string cfg_fname = argv[1];
    CHECK(!cfg_fname.empty());

    Client client(cfg_fname);
    client.Init();
    cout << "DONE -> Client->Init()" << endl;

    client.run_console();
    return 0;
}
