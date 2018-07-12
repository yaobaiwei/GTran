#pragma once

template <typename T>
class AbstractThreadSafeQueue {
public:
    virtual ~AbstractThreadSafeQueue() {}
    virtual void Push(T) = 0;
    virtual void WaitAndPop(T&) = 0;
    virtual int Size() = 0;
};
