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
    cout << "Client just posted a REQ" << endl;

    //get the available worker ID
    cc_.Recv(MASTER_RANK, um);
    um >> id;
    um >> handler;
    cout << "Client " << id << " recvs a REP: get available worker" << handler << endl;
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
    cout << "Client posts the query to worker" << handler << endl;

    cc_.Recv(handler, um);

    string result;
    um >> result;
    return result;
}

void Client::print_help(void)
{
    cout << "GQuery commands: " << endl;
    cout << "    help                display help infomation" << endl;
    cout << "    quit                quit from console" << endl;
    // cout << "    config <args>       run commands on config" << endl;
    // cout << "        -v                  print current config" << endl;
    // cout << "        -l <file>           load config items from <file>" << endl;
    // cout << "        -s <string>         set config items by <str> (format: item1=val1&item2=...)" << endl;
    cout << "    gquery <args>       run Gremlin-Like queries" << endl;
    cout << "        -q [QUERY]          a single query input by user" << endl;
    cout << "        -f <file> [<args>]  a single query from <file>" << endl;
    // cout << "           -n <num>            run <num> times" << endl;
    // cout << "           -v <num>            print at most <num> lines of results" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
    cout << "        -b <file> [<args>]  a set of queries configured by <file> (batch-mode)" << endl;
    cout << "           -o <file>           output results into <file>" << endl;
}

void Client::run_console() {
    
  // Timer:
  // 1. Whole time for user
  // 2. Touch Master to end
  // 3. Touch Worker to end
  while (true) {
next:
    string cmd;
    cout << "GQuery> ";
    std::getline(std::cin, cmd);

    // Timer
    uint64_t enter_t = timer::get_usec();

    // trim input

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
        bool f_enable = false, b_enable = false, o_enable = false;

        // get parameters
        while (cmd_ss >> token) {
          if (token == "-q") {
            // single query by command
            std::getline(cmd_ss, query);
            cout << "Query : " << query << endl;
            // touch_t = timer::get_usec();
            // client.RequestWorker();
            // string result = client.CommitQuery(query);
            goto next;
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

        if (!f_enable && !b_enable) goto failed; // meaningless

        if (f_enable) { // -f <file>
          cout << "f_enable" << endl;

          ifstream file(fname.c_str());
          if (!file) {
              cout << "ERROR: " << fname << " does not exist." << endl;
              goto next;
          }

          // read only one line
          std::getline(file, query);
          
          // timer
          // touch_t = timer::get_usec();

          // client.RequestWorker();
          // result = client.CommitQuery(query);
          cout << query << endl;
          result = "test";
          if (o_enable) {
            std::ofstream ofs(ofname, std::ofstream::out);
            ofs << result;
          } else {
            cout << result << endl;
          }
        }

        if (b_enable) { // -b <file>
          cout << "b_enable" << endl;

          ifstream file(bname.c_str());
          if (!file) {
              cout << "ERROR: " << fname << " does not exist." << endl;
              goto next;
          }

          string line;
          while (std::getline(file, line))
              query += line + " ";
          
          // timer
          // touch_t = timer::get_usec();
          // client.RequestWorker();
          cout << query << endl;
          result = "test";
          if (o_enable) {
            std::ofstream ofs(ofname, std::ofstream::out);
            ofs << result;
          } else {
            cout << result << endl;
          }
        }

      } else {
failed:
        cout << "Failed to run the command: " << cmd << endl;
        print_help();
      }

    } // run cmd

    uint64_t end_t = timer::get_usec();
    cout << (end_t - enter_t) / 100 << " ms for whole process." << endl;
    // cout << (end_t - touch_t) / 100 << " ms for whole process." << endl;
  } // outside while

} // run_console

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
