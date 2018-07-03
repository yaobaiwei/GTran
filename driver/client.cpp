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
		cout << "Client " << id << " recvs the response: get available worker" << handler << endl;
	}

	//use Message as a Query, if necessary, we can change
	//the same as result
	template <typename V>
	SArray<V> PostQuery(Message & msg){
		ibinstream m;
		obinstream um;

		char hostname[HOST_NAME_MAX];
		gethostname(hostname, HOST_NAME_MAX);
		string host_str(hostname);
		m << host_str;
		m << msg;
		cc_.Send(handler, m);
		cout << "Client posts the query to worker" << handler << endl;

		cc_.Recv(handler, um);

		SArray<V> result;
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
	google::InitGoogleLogging(argv[0]);
	string cfg_fname = argv[1];
	CHECK(!cfg_fname.empty());

	Client client(cfg_fname);
	client.Init();
	cout  << "DONE -> Client->Init()" << endl;

	client.RequestWorker();
	Message m;
	m.meta.msg_type = MSG_T::REPLY;
	m.meta.qid = 1;
	SArray<char> data;
	data.push_back('a');
	data.push_back('b');
	m.AddData(data);

	SArray<int> result = client.PostQuery<int>(m);
	cout << "Client recvs the result => SArray: size = " << result.size() << " first elem = " << result[0] << endl;
	return 0;
}
