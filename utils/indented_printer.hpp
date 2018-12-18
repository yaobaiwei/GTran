/*-----------------------------------------------------
       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-12
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/
#pragma once



#include <cstdio>
#include <string>
#include <cstdlib>
#include <stdio.h>
#include <iostream>
#include <list>
#include <memory.h>
#include <signal.h>

#include <stdarg.h>

namespace std
{

class IndentedPrinter
{
private:
    IndentedPrinter(){};
    IndentedPrinter(const IndentedPrinter&);//not to def
    IndentedPrinter& operator=(const IndentedPrinter&);//not to def
    ~IndentedPrinter()
    {
    }

    string indent_content_ = "  ";
    int current_indent_ = 0;

    void IncreaseIndent(){current_indent_++;}
    void DecreaseIndent(){current_indent_--;}

public:
    static IndentedPrinter& GetInstance()
    {
        static IndentedPrinter printer_single_instance;
        return printer_single_instance;
    }

    void SetIndentContent(string s){indent_content_ = s;}

    void Printf(const char * format, ...)
    {
        va_list args;
        va_start(args, format);

        static char s[1024 * 1024];
        int n = 0;

        for(int i = 0; i < current_indent_; i++)
        {
            n += sprintf(s + n, indent_content_.c_str());
        }

        vsprintf(s + n, format, args);

        printf(s);
        va_end(args);
    }

    class IndentWrapper
    {
    public:
        IndentWrapper()
        {
            // printf("IndentWrapper()\n");
            IndentedPrinter::GetInstance().IncreaseIndent();
            ;;;/*QAQ*/;;;
        }

        ~IndentWrapper()
        {
            // printf("~IndentWrapper()\n");
            IndentedPrinter::GetInstance().DecreaseIndent();
            ;;;/*OTZ*/;;;
        }
    };
};
}
