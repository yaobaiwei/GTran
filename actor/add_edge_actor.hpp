/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef ACTOR_ADD_EDGE_ACTOR_HPP_
#define ACTOR_ADD_EDGE_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "layout/data_storage.hpp"
#include "layout/pmt_rct_table.hpp"

class AddEdgeActor : public AbstractActor {
 public:
    AddEdgeActor(int id,
            int num_thread,
            int machine_id,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        machine_id_(machine_id),
        mailbox_(mailbox),
        type_(ACTOR_T::ADDE) {
        config_ = Config::GetInstance();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        id_mapper_ = SimpleIdMapper::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // Id Mapper
    // For check which side of edge stored in current machine
    SimpleIdMapper * id_mapper_;

    // RCT Table
    PrimitiveRCTTable * pmt_rct_table_;
};

#endif  // ACTOR_ADD_EDGE_ACTOR_HPP_
