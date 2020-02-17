/* Copyright 2019 Husky Data Lab, CUHK

Authors: Created by Changji LI (cjli@cse.cuhk.edu.hk)

*/

#ifndef EXPERT_DROP_EXPERT_HPP_
#define EXPERT_DROP_EXPERT_HPP_

#include <string>
#include <vector>

#include "base/core_affinity.hpp"
#include "base/type.hpp"
#include "core/message.hpp"
#include "core/factory.hpp"
#include "core/id_mapper.hpp"
#include "expert/abstract_expert.hpp"
#include "layout/data_storage.hpp"
#include "layout/index_store.hpp"
#include "layout/pmt_rct_table.hpp"
#include "utils/tool.hpp"

class DropExpert : public AbstractExpert {
 public:
    DropExpert(int id,
            int num_thread,
            int machine_id,
            AbstractMailbox * mailbox,
            CoreAffinity * core_affinity) :
        AbstractExpert(id, core_affinity),
        num_thread_(num_thread),
        machine_id_(machine_id),
        mailbox_(mailbox),
        type_(EXPERT_T::DROP) {
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

    // Expert type
    EXPERT_T type_;

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

    PROCESS_STAT processDrop(const QueryPlan & qplan, vector<pair<history_t, vector<value_t>>> & data, Element_T elem_type,
            bool isProperty, vector<uint64_t> & update_ids, vector<value_t> & update_vals, Primitive_T & pmt_type);
};

#endif  // EXPERT_DROP_EXPERT_HPP_