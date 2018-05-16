#pragma once

#include <queue> 

#include "core/message.hpp"
#include "base/abstract_mailbox.hpp"
#include "base/abstract_sender.hpp"

class Sender : public AbstractSender {
public:
	explicit Sender(int t_id, AbstractMailbox *mailbox, Meta msg_meta, int block_sz);
	std::queue<int>* GetQueue();

	// If the queue size is large enough, then send out
	bool BlockSendOut();
	// Send out anyway
	void SendOut();

private:
	int t_id_;
	Meta msg_meta_;
	std::queue<int> queue_;
	int block_sz_;
	// Not owned
	AbstractMailbox *mailbox_;
};
