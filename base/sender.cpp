#include "base/sender.hpp"

Sender::Sender(int t_id, AbstractMailbox *mailbox, Meta msg_meta, int block_sz) : 
    mailbox_(mailbox), msg_meta_(msg_meta), block_sz_(block_sz) {}

std::queue<int>* Sender::GetQueue() {
    return &queue_;
}

bool Sender::BlockSendOut() {
    if (queue_.size() >= block_sz_) {
        SendOut();
        return true;
    }
    return false;
}

void Sender::SendOut() {
    Message msg_to_send;
    msg_to_send.meta = msg_meta_;
    while (!queue_.empty()) {
        msg_to_send.fake_data.push_back(queue_.front());
        queue_.pop();
    }
    mailbox_->Send(t_id_, msg_to_send);
}

