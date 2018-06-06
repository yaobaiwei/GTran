/*
 * hw_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */

#ifndef HW_ACTOR_HPP_
#define HW_ACTOR_HPP_

#include <string>
#include "glog/logging.h"

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/node.hpp"
#include "utils/type.hpp"


class HwActor : public AbstractActor {
public:
    HwActor(int id, Node node, AbstractMailbox * mailbox) : AbstractActor(id),
    	node_(node), mailbox_(mailbox) { type_ = ACTOR_T::HW; }

    virtual ~HwActor(){}

    void process(int t_id, Message & msg) override {
        if (node_.id == 0){
            // create msg
            Message msg_to_send;
            msg_to_send.meta.sender = 0;
            msg_to_send.meta.recver = 1;
            msg_to_send.meta.qid = 0;
            msg_to_send.meta.step = 1;
            msg_to_send.meta.msg_type = MSG_T::FEED;
            msg_to_send.meta.chains.push_back(ACTOR_T::HW);

            SArray<int> send_data;
            send_data.push_back(4);
            send_data.push_back(9);

            msg_to_send.AddData(send_data);

            // send msg
            mailbox_->Send(t_id, msg_to_send);

        }else if (node_.id == 1) {
            Message msg_to_recv = mailbox_->Recv();

            // Do checking
            CHECK_EQ(msg_to_recv.meta.sender, 0);
            CHECK_EQ(msg_to_recv.meta.recver, 1);
            auto data = SArray<int>(msg_to_recv.data[0]);
            CHECK_EQ(data[0], 4);
            CHECK_EQ(data[1], 9);
        }
    }

private:
    // Actor type
    ACTOR_T type_;

    // Node affliated
    Node node_;

    // Handler of mailbox
    AbstractMailbox * mailbox_;

    // Ensure only one thread ever runs the actor
    std::mutex thread_mutex_;
};




#endif /* HW_ACTOR_HPP_ */
