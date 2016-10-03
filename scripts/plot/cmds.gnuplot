# paramaters:
# x_label, y_label, graph_title, input_files, colors, output_file, terminal_type, 
# x_tics, y_tics, x_range, y_range, x_col, y_col, key_locations, using_cmd
set size ratio 0.5625

if (output_type eq "ps") {
    set terminal postscript  color colortext "Helvetica,30" eps
	#set terminal postscript "Helvetica" eps font 22
    if (!exists("output_file")) {
        output_file = "out.eps"
    }
} else {
    set terminal pngcairo size @graph_size font "Helvetica,12"
    if (!exists("output_file")) {
        output_file = "out.png"
    }
}

if (!exists("colors"))        colors = " #666600 #AA1111 #11AA11 #1111AA #AA11AA #11AAAA"
if (!exists("graph_title"))   graph_title = ""
if (!exists("terminal_type")) terminal_type = "pngcairo"
if (!exists("x_label"))       x_label = ""
if (!exists("y_label"))       y_label = ""
if (!exists("using_cmd"))     using_cmd = "6:7"
if (!exists("x_range"))       set autoscale x
if (!exists("y_range"))       set autoscale y
if (!exists("key_location"))  key_location = "top right"
if (!exists("style"))         style = ""
if (!exists("output_type"))   output_type = "png"

set output output_file

set tics nomirror
set style line 11 lc rgb '#000000' lt 1
set border 3 back ls 11
set tics nomirror
# define grid
set style line 12 lc rgb '#808080' lt 0 lw 1
set grid back ls 12

symbols = ""
set macros
set autoscale
#set terminal terminal_type
set datafile separator ','
set title graph_title
set xlabel x_label
set ylabel y_label
set key @key_location
if (exists("x_tics")) set xtics @x_tics
if (exists("y_tics")) set ytics @y_tics
if (exists("x_range")) set xrange @x_range
if (exists("y_range")) set yrange @y_range
if (exists("log_scale")) set logscale @log_scale
if (exists("y_format")) set format y @y_format

set linetype 1 lc rgb word(colors, 1)
set linetype 2 lc rgb word(colors, 2) dashtype (5,1,5,1,5,2)
set linetype 3 lc rgb word(colors, 3) dashtype (5,2,5,2)
set linetype 4 lc rgb word(colors, 4) dashtype (3,3,3,3)
set linetype cycle 4

plot for [i=1:words(input_files)] \
word(input_files, i) using @using_cmd \
title column(2) \
with linespoints pt (i+5 % 50) ps 3 lt i lw 5
