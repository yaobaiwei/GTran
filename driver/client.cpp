/*
 * client.cpp
 *
 *  Created on: Jun 24, 2018
 *      Author: Hongzhi Chen
 */

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "base/sarray.hpp"
#include "base/type.hpp"
#include "base/serialization.hpp"
#include "base/client_connection.hpp"
#include "utils/global.hpp"
#include "core/message.hpp"

#include "glog/logging.h"

#include <unistd.h>
#include <limits.h>

class Client{
public:
	Client(string cfg_fname): fname_(cfg_fname){
		id = -1;
	}

	void Init(){
		nodes_ = ParseFile(fname_);
		CHECK(CheckUniquePort(nodes_));
		master_ = GetNodeById(nodes_, 0);
		cc_.Init(nodes_);
	}

	void RequestWorker(){
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


	string CommitQuery(string query){
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

private:
	int id;
	string fname_;
	vector<Node> nodes_;
	Node master_;
	ClientConnection cc_;
	int handler;
};

//prog node-config-fname_path host-fname_path
int main(int argc, char* argv[])
{
	if(argc != 3){
		cout << "2 params required" <<endl;
		return 0;
	}
	google::InitGoogleLogging(argv[0]);
	string cfg_fname = argv[1];
	CHECK(!cfg_fname.empty());

	Client client(cfg_fname);
	client.Init();
	cout  << "DONE -> Client->Init()" << endl;

	client.RequestWorker();
	string query = argv[2];
	string result = client.CommitQuery(query);
	cout << result << endl;
	return 0;
}
