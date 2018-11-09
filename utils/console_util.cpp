/*-----------------------------------------------------

       @copyright (c) 2018 CUHK Husky Data Lab
              Last modified : 2018-11
  Author(s) : Chenghuan Huang(entityless@gmail.com)
:)
-----------------------------------------------------*/

#include "console_util.hpp"

using namespace std;

int ConsoleUtil::GetKey()
{
    struct termios oldattr, newattr;
    int ch;
    tcgetattr( STDIN_FILENO, &oldattr );
    newattr = oldattr;
    newattr.c_lflag &= ~( ICANON | ECHO );
    tcsetattr( STDIN_FILENO, TCSANOW, &newattr );
    ch = getchar();
    tcsetattr( STDIN_FILENO, TCSANOW, &oldattr );
    return ch;
}

void ConsoleUtil::SetColor(out_colors cl)
{
    switch(cl)
    {
    case K_RED:
        fputs("\x1B[1;31m", stdout);
        break;
    case K_GREEN:
        fputs("\x1B[1;32m", stdout);
        break;
    case K_BLUE:
        fputs("\x1B[1;34m", stdout);
        break;
    case K_YELLOW:
        fputs("\x1B[1;33m", stdout);
        break;
    case K_CYAN:
        fputs("\x1B[1;36m", stdout);
        break;
    case K_MAGENTA:
        fputs("\x1B[1;35m", stdout);
        break;
    case K_WHITE:
        fputs("\x1B[1;37m", stdout);
        break;
    default:
        break;
    }
}

void ConsoleUtil::ResetColor()
{
    fputs("\x1B[0m", stdout);
}

ConsoleUtil::ConsoleUtil()
{
    memset(line_length_, 0, sizeof(int) * BUFFER_LINE);
}

void ConsoleUtil::LoadHistory()
{

    //map to the actually line
    int actual_line_no = (cur_line_no_ - cur_roll_no_ + BUFFER_LINE) % BUFFER_LINE;

    //fetch the line into the buffer
    memcpy((void*)buffer_, (void*)line_bfs_[actual_line_no], BUFFER_SIZE);
    cur_line_len_ = line_length_[actual_line_no];

    ClearLine();
}

void ConsoleUtil::OnKeyUp()
{
    // printf("\nvoid ConsoleUtil::OnKeyUp()\n");
    //
    int max_key = BUFFER_LINE - 1;

    //when the cur_line_no_ is 98 (and BUFFER_LINE == 100), then 98 history slots are available
    if(cur_line_no_ < BUFFER_LINE)
    {
        max_key = cur_line_no_;
    }

    if(cur_roll_no_ == max_key)
    {
        return;//do nothing, for that cannot roll up
    }

    if(cur_roll_no_ == 0)
    {
        //buffer the editing line into the line_bfs_
        memcpy((void*)line_bfs_[cur_line_ptr_no_], (void*)buffer_, BUFFER_SIZE);
        // printf("memcpy(line_bfs_[%d], buffer_)\n", cur_line_no_ % BUFFER_LINE);
        line_length_[cur_line_ptr_no_] = cur_line_len_;
    }

    //increase the roll no
    cur_roll_no_++;

    LoadHistory();
}

void ConsoleUtil::OnKeyDown()
{
    // printf("\nvoid ConsoleUtil::OnKeyDown()\n");

    //if cur_roll_no_ becomes 0
    if(cur_roll_no_ == 0)
        return;

    cur_roll_no_--;

    LoadHistory();
}

void ConsoleUtil::OnKeyLeft()
{
    // printf("\nvoid ConsoleUtil::OnKeyLeft()\n");

}

void ConsoleUtil::OnKeyRight()
{
    // printf("\nvoid ConsoleUtil::OnKeyRight()\n");

}

string ConsoleUtil::FetchConsoleResult()
{
    //generate result
    string s(buffer_);

    //copy into the history
    memcpy((void*)line_bfs_[cur_line_ptr_no_], (void*)buffer_, BUFFER_SIZE);
    line_length_[cur_line_ptr_no_] = cur_line_len_;
    // printf("memcpy(line_bfs_[%d], buffer_)\n", cur_line_no_);

    //increase the seq
    cur_line_no_++;
    cur_line_ptr_no_ = cur_line_no_ % BUFFER_LINE;

    //disable the current 
    memset(buffer_, 0, BUFFER_SIZE);

    return s;
}

void ConsoleUtil::ClearLine()
{
    cout << '\r';//this moves the cursor to the head of the line
    printf("%c[2K", 27);//this clears the line
    cout<<line_head_;
    printf("%s", buffer_);
}

string ConsoleUtil::TryConsoleInput(string line_head)
{
    cur_roll_no_ = 0;
    cur_line_len_ = 0;

    line_head_ = line_head;
    ClearLine();

    const int ESC_KEY = 27;
    const int ENTER = 13;
    const int CTRLC = 3;
    const int CTRLD = 4;
    const int BACKSPACE_KEY = 8;

    const int DOWN_KEY = 258;
    const int UP_KEY = 259;
    const int LEFT_KEY = 260;
    const int RIGHT_KEY = 261;

    const int HOME_KEY = 262;
    
    int c, i = 0;

    while(true)
    {
        int key = GetKey();
        if(isprint(key))
        {
            printf("%c", key);
            sprintf(&buffer_[cur_line_len_++], "%c", key);
        }
        else
        {
            // printf("\n%s", buffer_);

            //The coding philosophy of The Scroll Of Taiwu.
            if(key == 8)//backspace
            {
                if(cur_line_len_ > 0)
                    buffer_[--cur_line_len_] = 0;
                // tcflush(0, TCOFLUSH);//failed
                ClearLine();
            }
            else if(key == 10)//enter
            {
                // *line_identifer_.begin();

                //todo: only legal argument will be pushed to the history
                return FetchConsoleResult();
            }
            else if(key == 27)
            {
                int key2 = GetKey();
                if(key2 == 91)
                {
                    int key3 = GetKey();
                    if(key3 == 65)//up
                    {
                        OnKeyUp();
                    }
                    else if(key3 == 66)//down
                    {
                        OnKeyDown();
                    }
                    else if(key3 == 67)//right
                    {
                        OnKeyRight();
                    }
                    else if(key3 == 68)//left
                    {
                        OnKeyLeft();
                    }
                    else if(key3 == 49)
                    {
                        int key4 = GetKey();
                        if(key4 == 126)//home
                        {

                        }
                    }
                    else if(key3 == 52)
                    {
                        int key4 = GetKey();
                        if(key4 == 126)//end
                        {
                            
                        }
                    }
                    else if(key3 == 51)
                    {
                        int key4 = GetKey();
                        if(key4 == 126)//delete
                        {
                            
                        }
                    }
                }
            }
        }

        // cout<<"<< "<<key<<" >>"<<endl;
        // << 27 >><< 91 >><< 65 >> up
        // << 27 >><< 91 >><< 68 >> left
        // << 27 >><< 91 >><< 66 >> down
        // << 27 >><< 91 >><< 67 >> right
        // << 27 >><< 91 >><< 49 >><< 126 >> home
        // << 27 >><< 91 >><< 52 >><< 126 >> end
        // << 27 >><< 91 >><< 51 >><< 126 >> delete
        // << 27 >> Esc //it is so bad :(
        // << 8 >> backspace
        // << 10 >> enter
    }

    return string(buffer_);
}