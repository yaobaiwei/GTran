#pragma once

#include "core/message.hpp"

#include <string>

class AbstractMailbox {
public:
  virtual ~AbstractMailbox() {}

  virtual int Send(const int t_id, const Message & msg) = 0;
  virtual bool TryRecv(Message & msg) = 0;
  virtual Message Recv() = 0;
};
