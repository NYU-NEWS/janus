#!/bin/bash
PROC_NAME=${1-"deptran_server"}
SESSION_NAME=${2-"janus_debug"}

echo "create tmux session in $SESSION_NAME"

pids=`ps aux | grep "$PROC_NAME" | grep -v grep | awk '{print $2}'`
echo "Attach to pids $(echo $pids | tr -d '\r\n')"

attach_gdb() {
	cmd="gdb -p $1" 
	tmux split-window -t "$SESSION_NAME" "$cmd"
}

tmux new-session -s "$SESSION_NAME" -d 

while read -r pid; do
	attach_gdb $pid
done <<< "$pids"

tmux kill-pane -t "$SESSION_NAME:0.0"
tmux select-layout -t "$SESSION_NAME:0" even-vertical 
tmux attach -t "$SESSION_NAME" || (echo "no processes found" && ps aux | grep "$PROC_NAME" && exit 1)
