/*
 * hw_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */

#ifndef HW_ACTOR_HPP_
#define HW_ACTOR_HPP_

#include <string>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/node.hpp"
#include "utils/type.hpp"
#include "actor/abstract_actor.hpp"

#include "glog/logging.h"


class HwActor : private AbstractActor {
public:
    HwActor(int id, Node node, AbstractMailbox * mailbox) :
        id_(id), node_(node), mailbox_(mailbox) { type_ = ACTOR_T::HW; }

    virtual ~HwActor(){}

    virtual const int GetActorId() override {
        return id_;
    }

    template <class T>
    virtual void process(int t_id, Message<T>& msg) override {
        if (node_.id == 0){
            // create msg
            Message<T> msg_to_send;
            msg_to_send.meta.sender_node = 0;
            msg_to_send.meta.recver_node = 1;
            msg_to_send.meta.query_id = 0;
            msg_to_send.meta.step = 1;
            msg_to_send.meta.msg_type = MSG_T::FEED;
            msg_to_send.meta.qlink.push_back(ACTOR_T::HW);

            msg_to_send.AddData(4);
            msg_to_send.AddData(9);

            // send msg
            mailbox_->Send(t_id, msg_to_send.meta.recver_node, msg_to_send);

        }else if (node_.id == 1) {
            Message<T> msg_to_recv = mailbox_->Recv<T>();

            // Do checking
            CHECK_EQ(msg_to_recv.meta.sender_node, 0);
            CHECK_EQ(msg_to_recv.meta.recver_node, 1);
            CHECK_EQ(msg_to_recv.data[0], 4);
            CHECK_EQ(msg_to_recv.data[1], 9);
        }
    }

private:
  // Actor ID
  int id_;

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
