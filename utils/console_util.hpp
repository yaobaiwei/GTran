/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once



#include <cstdio>
#include <string>
#include <cstdlib>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>
// #include <stdarg.h>
// #include <ncurses.h>

// #include "conio.h"

#define BLUE COLOR_BLUE
#define RED COLOR_RED
#define WHITE COLOR_WHITE
#define BLACK COLOR_BLACK
#define MAGENTA COLOR_MAGENTA
#define CYAN COLOR_CYAN
#define GREEN COLOR_GREEN
#define YELLOW COLOR_YELLOW
#define LIGHTBLUE COLOR_BLUE
#define LIGHTRED COLOR_RED
#define LIGHTGREEN COLOR_GREEN
#define _cu_get_color_pair(f,b) ((f)|((b)<<3))

//please do not press Esc button, which will causes bug.
class ConsoleUtil
{
private:
    //inner record of the input seq
    //non empty ENTER will increase this by 1
    int cur_line_no_ = 0;
    int cur_line_ptr_no_ = 0;//always == cur_line_no_ % BUFFER_LINE
    int cur_line_len_;

    int cur_roll_no_; //the distance of buffered line

    static const int BUFFER_SIZE = 20480;//20KB
    static const int BUFFER_LINE = 100;//99 line for history, 1 line for current

    char line_bfs_[BUFFER_LINE][BUFFER_SIZE];
    int line_length_[BUFFER_LINE];
    char buffer_[BUFFER_SIZE];//the actual line being edited
    // char** line_bfs_;

    std::list<int> line_identifer_;
    std::list<int>::iterator cur_using_buffer_line_;//this will be set to 0 at the start of each query

    //when 
    // char buffer_;

    termios ori_term_attr_;
    ConsoleUtil(const ConsoleUtil&);//not to def
    ConsoleUtil& operator=(const ConsoleUtil&);//not to def
    ~ConsoleUtil()
    {
        printf("ConsoleUtil::~ConsoleUtil()\n");
        fflush(stdout);
        tcsetattr( STDIN_FILENO, TCSANOW, &ori_term_attr_ );
    }
    ConsoleUtil()
    {
        printf("ConsoleUtil::ConsoleUtil()\n");
        memset(line_length_, 0, sizeof(int) * BUFFER_LINE);
        tcgetattr( STDIN_FILENO, &ori_term_attr_ );
        ori_term_attr_.c_lflag |= ICANON | ECHO;

        // signal(SIGINT, (__sighandler_t {aka void (*)(int)})ConsoleUtil::signal_ctrlc); 
        signal(SIGINT, ConsoleUtil::signal_ctrlc); 
    }

    static void signal_ctrlc(int sig)
    {
        exit(0);
    }

    int GetKey();
    void ClearLine();
    void LoadHistory();

    void OnKeyUp();
    void OnKeyDown();
    void OnKeyLeft();
    void OnKeyRight();

    std::string FetchConsoleResult();

    std::string line_head_;

public:
    static ConsoleUtil& GetInstance()
    {
        static ConsoleUtil console_single_instance;
        return console_single_instance;
    }

    std::string TryConsoleInput(std::string line_head = "");

    enum out_colors { K_RED, K_GREEN, K_BLUE, K_YELLOW, K_CYAN, K_MAGENTA, K_WHITE, K_NONE };
    void SetColor(out_colors);
    void ResetColor();
};

