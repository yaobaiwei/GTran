/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include "core/message.hpp"
#include "base/node.hpp"

#include <string>

class AbstractMailbox {
public:
  virtual ~AbstractMailbox() {}

  struct mailbox_data_t{
	  ibinstream stream;
	  int dst_nid;
	  int dst_tid;
  };

  virtual void Init(vector<Node> & nodes) = 0;
  virtual int Send(int tid, const Message & msg) = 0;
  virtual int Send(int tid, const mailbox_data_t & data) = 0;
  virtual bool TryRecv(int tid, Message & msg) = 0;
  virtual void Recv(int tid, Message & msg) = 0;
  virtual void Sweep(int tid) = 0;
  virtual void Send_Notify(int dst_nid, ibinstream& in) = 0;
  virtual void Recv_Notify(obinstream& out) = 0;
};
