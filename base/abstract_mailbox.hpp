#pragma once

#include "core/message.hpp"
#include "base/node.hpp"

#include <string>

template <class T>
class AbstractMailbox {
public:
  virtual ~AbstractMailbox() {}
  virtual void Init(std::string f_name) = 0;
  virtual void Start() = 0;
  virtual int Send(const int t_id, const int dst_nid, const Message<T>& msg) = 0;
  virtual Message<T> Recv() = 0;
  virtual void Stop() = 0;
};
