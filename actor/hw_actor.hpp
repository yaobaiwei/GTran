/*
 * hw_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */

#ifndef HW_ACTOR_HPP_
#define HW_ACTOR_HPP_

#include <iostream>
#include <string>
#include "glog/logging.h"

#include "actor/abstract_actor.hpp"
#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/node.hpp"
#include "base/type.hpp"

using namespace std;

class HwActor : public AbstractActor {
public:
    HwActor(int id, Node node, AbstractMailbox * mailbox) : AbstractActor(id),
    	node_(node), mailbox_(mailbox) { type_ = ACTOR_T::HW; }

    virtual ~HwActor(){}

    void process(int tid, Message & msg){
    	//TEST
    	cout << "RANK:" << node_.get_local_rank() << "tid:" << tid << " => MSG FROM " << msg.meta.sender_nid << ":" << msg.meta.sender_tid
    			<< " to "  << msg.meta.recver_nid << ":" << msg.meta.recver_tid << " => DATA-:" << msg.data[0].first.size() << "|" << msg.data[0].second.size() << endl;
    }

private:
    // Actor type
    ACTOR_T type_;

    // Node
    Node node_;

    // Handler of mailbox
    AbstractMailbox * mailbox_;

    // Ensure only one thread ever runs the actor
    std::mutex thread_mutex_;
};




#endif /* HW_ACTOR_HPP_ */
