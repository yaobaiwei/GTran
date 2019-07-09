/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef ACTOR_DROP_ACTOR_HPP_
#define ACTOR_DROP_ACTOR_HPP_

#include <string>
#include <vector>

#include "actor/abstract_actor.hpp"
#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/id_mapper.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/tool.hpp"

class DropActor : public AbstractActor {
 public:
    DropActor(int id,
            int num_thread,
            int machine_id,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractActor(id, core_affinity),
        num_thread_(num_thread),
        machine_id_(machine_id),
        mailbox_(mailbox),
        type_(ACTOR_T::DROP) {
        config_ = Config::GetInstance();
        trx_table_stub_ = TrxTableStubFactory::GetTrxTableStub();
        pmt_rct_table_ = PrimitiveRCTTable::GetInstance();
        id_mapper_ = SimpleIdMapper::GetInstance();
        index_store_ = IndexStore::GetInstance();
    }

    void process(const QueryPlan & qplan, Message & msg);

 private:
    // Node Info 
    int num_thread_;
    int machine_id_;

    // Actor type
    ACTOR_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;
    Config* config_;

    // TrxTableStub
    TrxTableStub * trx_table_stub_;

    // Id Mapper
    // For check which side of edge stored in current machine
    SimpleIdMapper * id_mapper_;

    // RCT Table
    PrimitiveRCTTable * pmt_rct_table_;

    // Index Store
    IndexStore * index_store_;

    bool processDrop(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, Element_T elem_type,
            bool isProperty, vector<uint64_t> & update_ids, vector<value_t> & update_vals, Primitive_T & pmt_type);
};

#endif  // ACTOR_DROP_ACTOR_HPP_
