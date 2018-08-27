set terminal png transparent size 640,240
set size 1.0,1.0

set terminal png transparent size 640,480
set output 'lines_of_code_by_author.png'
set key left top
set xdata time
set timefmt "%s"
set format x "%Y-%m-%d"
set grid y
set ylabel "Lines"
set xtics rotate
set bmargin 6
plot 'lines_of_code_by_author.dat' using 1:2 title "nickfang" w lines, 'lines_of_code_by_author.dat' using 1:3 title "Changji" w lines, 'lines_of_code_by_author.dat' using 1:4 title "hzchen" w lines, 'lines_of_code_by_author.dat' using 1:5 title "Aaron LI" w lines, 'lines_of_code_by_author.dat' using 1:6 title "Hongzhi Chen" w lines
