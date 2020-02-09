/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef EXPERT_ADD_EDGE_EXPERT_HPP_
#define EXPERT_ADD_EDGE_EXPERT_HPP_

#include <string>
#include <vector>

#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"

class AddEdgeExpert : public AbstractExpert {
 public:
    AddEdgeExpert(int id,
            int num_thread,
            int machine_id,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        machine_id_(machine_id),
        mailbox_(mailbox),
        type_(EXPERT_T::ADDE) {
        config_ = Config::GetInstance();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        id_mapper_ = SimpleIdMapper::GetInstance();
        index_store_ = IndexStore::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Number of Threads
    int num_thread_;
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // Id Mapper
    // For check which side of edge stored in current machine
    SimpleIdMapper * id_mapper_;

    // RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // Index Store
    IndexStore * index_store_;
};

#endif  // EXPERT_ADD_EDGE_EXPERT_HPP_
