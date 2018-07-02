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

	void Request(){
		ibinstream m;
		obinstream um;
		m << id;
		cc_.Send(MASTER_RANK, m);
		cout << "Client posts a request" << endl;

		cc_.Recv(MASTER_RANK, um);
		um >> id;
		um >> handler;
		cout << "Client " << id << " recvs a response: Handler=> " << handler << endl;
	}

	//use Message as a Query, if necessary, we can change
	//the same as result
	template <typename V>
	SArray<V> PostQuery(Message & msg){
		ibinstream m;
		obinstream um;
		m << msg;
		cc_.Send(handler, m);
		cout << "Client posts the query to " << handler << endl;

		cc_.Recv(handler, um);

		SArray<int> result;
		um >> result;
		cout << "Client recvs the result in form SArray: Size = " << result.size() << " First Elem = " << result[0] << endl;
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

	client.Request();
	Message m;
	m.meta.msg_type = MSG_T::REPLY;
	m.meta.qid = 1;
	SArray<int> data;
	data.push_back(1);
	data.push_back(2);
	m.AddData(data);

	SArray<int> re = client.PostQuery<int>(m);

	return 0;
}
