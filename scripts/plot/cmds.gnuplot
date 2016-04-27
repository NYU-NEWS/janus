# paramaters:
# x_label, y_label, graph_title, input_files, colors, output_file, terminal_type, 
# x_tics, y_tics, x_range, y_range, x_col, y_col, key_locations, using_cmd

if (!exists("colors"))        colors = "#AA1111 #11AA11 #1111AA #AAAA11 #AA11AA #11AAAA"
if (!exists("graph_title"))   graph_title = ""
if (!exists("output_file"))   output_file = "out.png"
if (!exists("terminal_type")) terminal_type = "pngcairo"
if (!exists("x_label"))       x_label = ""
if (!exists("y_label"))       y_label = ""
if (!exists("using_cmd"))     using_cmd = "6:7"
if (!exists("x_range"))       set autoscale x
if (!exists("y_range"))       set autoscale y
if (!exists("key_location"))  key_location = "top right"
if (!exists("style"))         style = ""

symbols = ""
set macros
set autoscale
set terminal terminal_type
set output output_file
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

plot for [i=1:words(input_files)] \
word(input_files, i) using @using_cmd \
title column(2) \
with linespoints pt (i % 50) ps 2 lt rgb word(colors, i)
