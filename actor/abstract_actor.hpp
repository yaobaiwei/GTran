/*
 * abstract_actor.hpp
 *
 *  Created on: May 26, 2018
 *      Author: Hongzhi Chen
 */

#ifndef ABSTRACT_ACTOR_HPP_
#define ABSTRACT_ACTOR_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"

template <class T>
class AbstractActor {
public:
  virtual ~AbstractActor() {}

  virtual const int GetActorId() = 0;

  virtual void process(int t_id, Message<T> & msg) = 0;

private:
};

#endif /* ABSTRACT_ACTOR_HPP_ */
